use std::{
    sync::Arc,
    task::{ready, Poll},
};

use crate::{
    stream::RecordBatchStreamAdapter,
    umami::{
        buffer::{LazyPartitionBuffer, LazyPartitionedSource, PartitionedSource, Sink},
        BasicStreamProvider,
    },
    utils::RowExpr,
};
use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{stream::BoxStream, Stream, StreamExt};

use super::StreamFactory;

/// MaterializeWrapper loads the input data into the buffer.
pub struct MaterializeWrapper<Buffer> {
    factory: Box<dyn StreamFactory + Send>,
    input: InputKind,
    partition: usize,
    ctx: Arc<TaskContext>,
    buffer: Buffer,
}

pub enum InputKind {
    Unary {
        input: SendableRecordBatchStream,
        expr: RowExpr,
    },
    Binary {
        left: SendableRecordBatchStream,
        left_expr: RowExpr,
        right: SendableRecordBatchStream,
        right_expr: RowExpr,
    },
    Placeholder,
}

impl InputKind {
    pub fn unary(input: SendableRecordBatchStream, expr: impl Into<RowExpr>) -> Self {
        Self::Unary {
            input,
            expr: expr.into(),
        }
    }

    pub fn binary(
        left: SendableRecordBatchStream,
        left_expr: impl Into<RowExpr>,
        right: SendableRecordBatchStream,
        right_expr: impl Into<RowExpr>,
    ) -> Self {
        Self::Binary {
            left,
            left_expr: left_expr.into(),
            right,
            right_expr: right_expr.into(),
        }
    }
}

impl<Buffer> MaterializeWrapper<Buffer> {
    pub fn new(
        factory: Box<dyn StreamFactory + Send>,
        input: InputKind,
        partition: usize,
        ctx: Arc<TaskContext>,
        buffer: Buffer,
    ) -> Self {
        Self {
            factory,
            input,
            partition,
            ctx,
            buffer,
        }
    }
}

type Output = genawaiter::sync::Co<Result<RecordBatch>>;

impl<Buffer: LazyPartitionBuffer + Send + 'static> MaterializeWrapper<Buffer> {
    pub fn stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.factory.output_schema(),
            genawaiter::sync::Gen::new(|co| self.run_wrapped(co)),
        ))
    }

    async fn run_wrapped(self, mut output: Output) {
        let run_result = self.run(&mut output).await;

        if let Err(e) = run_result {
            log::debug!("Materialize encoutered err: {:?}", e);
            let _ = output.yield_(Err(e)).await;
        }
    }

    async fn produce(
        mut stream: SendableRecordBatchStream,
        output: &mut Output,
    ) -> Result<()> {
        while let Some(batch) = stream.next().await {
            output.yield_(Ok(batch?)).await;
        }
        Ok(())
    }

    async fn assemble_and_produce(
        &mut self,
        inputs: Box<[SendableRecordBatchStream]>,
        output: &mut Output,
    ) -> Result<()> {
        let mut inputs = BasicStreamProvider::new(inputs);
        let inner = self.factory.make(&mut inputs, self.partition, &self.ctx)?;
        Self::produce(inner, output).await
    }

    async fn buffer(
        mut stream: SendableRecordBatchStream,
        sink: &mut Buffer::Sink,
    ) -> Result<()> {
        while let Some(batch) = stream.next().await {
            sink.push(batch?).await?;
        }
        Ok(())
    }

    async fn run(mut self, output: &mut Output) -> Result<()> {
        match std::mem::replace(&mut self.input, InputKind::Placeholder) {
            InputKind::Unary { input, expr } => self.run_unary(output, input, expr).await,
            InputKind::Binary {
                left,
                left_expr,
                right,
                right_expr,
            } => {
                self.run_binary(output, left, left_expr, right, right_expr)
                    .await
            }
            InputKind::Placeholder => {
                panic!("Placeholder encoutered during materialize execution")
            }
        }
    }

    async fn run_unary(
        mut self,
        output: &mut Output,
        stream: SendableRecordBatchStream,
        expr: RowExpr,
    ) -> Result<()> {
        let mut sink = Buffer::make_sink(&mut self.buffer, stream.schema(), expr)?;
        Self::buffer(stream, &mut sink).await?;
        let report = Buffer::probe_sink(&self.buffer, &sink);

        if report.parts_oom.is_empty() {
            let mut source = Buffer::make_source(&mut self.buffer, sink).await?;
            Self::assemble_and_produce(
                &mut self,
                Box::new([source.all_in_mem().await?]),
                output,
            )
            .await
        } else {
            sink.force_partition().await?;
            let mut source = Buffer::make_source(&mut self.buffer, sink)
                .await?
                .into_partitioned();

            for partition in report.parts_in_mem {
                let stream = source.stream_partition(partition).await?;
                Self::assemble_and_produce(&mut self, Box::new([stream]), output).await?;
            }

            for partition in report.parts_oom {
                let stream = source.stream_partition(partition).await?;
                Self::assemble_and_produce(&mut self, Box::new([stream]), output).await?;
            }

            Ok(())
        }
    }

    async fn run_binary(
        mut self,
        output: &mut Output,
        left: SendableRecordBatchStream,
        left_expr: RowExpr,
        right: SendableRecordBatchStream,
        right_expr: RowExpr,
    ) -> Result<()> {
        let mut left_sink =
            Buffer::make_sink(&mut self.buffer, left.schema(), left_expr)?;
        Self::buffer(left, &mut left_sink).await?;
        let report = Buffer::probe_sink(&mut self.buffer, &left_sink);

        if report.parts_oom.is_empty() {
            let mut left_source =
                Buffer::make_source(&mut self.buffer, left_sink).await?;

            Self::assemble_and_produce(
                &mut self,
                Box::new([left_source.all_in_mem().await?, right]),
                output,
            )
            .await
        } else if report.parts_in_mem.is_empty() {
            left_sink.force_partition().await?;
            let mut left_source = Buffer::make_source(&mut self.buffer, left_sink)
                .await?
                .into_partitioned();

            let mut right_sink =
                Buffer::make_sink(&mut self.buffer, right.schema(), right_expr)?;
            Self::buffer(right, &mut right_sink).await?;
            right_sink.force_partition().await?;
            let mut right_source = Buffer::make_source(&mut self.buffer, right_sink)
                .await?
                .into_partitioned();

            for part_idx in report.parts_oom {
                let left_stream = left_source.stream_partition(part_idx).await?;
                let right_stream = right_source.stream_partition(part_idx).await?;
                let provider = Box::new([left_stream, right_stream]);
                Self::assemble_and_produce(&mut self, provider, output).await?;
            }

            Ok(())
        } else {
            left_sink.force_partition().await?;

            let (right_stream, airlock) = Buffer::make_filtered_stream(
                &mut self.buffer,
                &left_sink,
                right,
                right_expr,
            )?;

            let mut left_source = Buffer::make_source(&mut self.buffer, left_sink)
                .await?
                .into_partitioned();

            let left_stream = left_source.stream_partitions(&report.parts_in_mem).await?;

            let provider = Box::new([left_stream, right_stream]);
            Self::assemble_and_produce(&mut self, provider, output).await?;

            let right_sink = airlock.get();
            let mut right_source = Buffer::make_source(&mut self.buffer, right_sink)
                .await?
                .into_partitioned();

            for part_idx in report.parts_oom {
                let left_stream = left_source.stream_partition(part_idx).await?;
                let right_stream = right_source.stream_partition(part_idx).await?;
                let provider = Box::new([left_stream, right_stream]);
                Self::assemble_and_produce(&mut self, provider, output).await?;
            }

            Ok(())
        }
    }
}

struct SelectAll<T> {
    streams: Vec<BoxStream<'static, T>>,
}

impl<T> Stream for SelectAll<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ref mut stream) = self.streams.last_mut() {
                match ready!(stream.poll_next_unpin(cx)) {
                    Some(item) => {
                        return Poll::Ready(Some(item));
                    }
                    None => {
                        self.streams.pop();
                    }
                }
            } else {
                return Poll::Ready(None);
            }
        }
    }
}
