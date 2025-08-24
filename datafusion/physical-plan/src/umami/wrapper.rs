use std::{
    sync::Arc,
    task::{ready, Poll},
};

use crate::{
    stream::RecordBatchStreamAdapter,
    umami::{
        buffer::{
            self, LazyPartitionBuffer, LazyPartitionedSource, PartitionIdx,
            PartitionedSource, Sink,
        },
        BasicStreamProvider,
    },
};
use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExprRef;
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

pub type DefaultMaterializeWrapper = MaterializeWrapper<buffer::IoUringSpillBuffer>;

pub enum InputKind {
    Unary {
        input: SendableRecordBatchStream,
        expr: Vec<PhysicalExprRef>,
    },
    Binary {
        left: SendableRecordBatchStream,
        left_expr: Vec<PhysicalExprRef>,
        right: SendableRecordBatchStream,
        right_expr: Vec<PhysicalExprRef>,
    },
    Placeholder,
}

impl InputKind {
    pub fn unary(input: SendableRecordBatchStream, expr: Vec<PhysicalExprRef>) -> Self {
        Self::Unary { input, expr }
    }

    pub fn binary(
        left: SendableRecordBatchStream,
        left_expr: Vec<PhysicalExprRef>,
        right: SendableRecordBatchStream,
        right_expr: Vec<PhysicalExprRef>,
    ) -> Self {
        Self::Binary {
            left,
            left_expr,
            right,
            right_expr,
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
        _expr: Vec<PhysicalExprRef>,
    ) -> Result<()> {
        let mut sink = Buffer::make_sink(&mut self.buffer, stream.schema())?;
        Self::buffer(stream, &mut sink).await?;
        let mut source = Buffer::make_source(&mut self.buffer, sink).await?;
        Self::assemble_and_produce(
            &mut self,
            Box::new([source.unpartitioned().await?]),
            output,
        )
        .await?;

        if self.buffer.partition_count() > 0 {
            let mut source = source.into_partitioned();
            for partition in 0..self.buffer.partition_count() {
                let stream = source.stream_partition(PartitionIdx(partition)).await;
                Self::assemble_and_produce(&mut self, Box::new([stream]), output).await?;
            }
        }
        Ok(())
    }

    async fn run_binary(
        mut self,
        output: &mut Output,
        left: SendableRecordBatchStream,
        _left_expr: Vec<PhysicalExprRef>,
        right: SendableRecordBatchStream,
        _right_expr: Vec<PhysicalExprRef>,
    ) -> Result<()> {
        assert!(self.buffer.partition_count() == 0);

        let mut left_sink = Buffer::make_sink(&mut self.buffer, left.schema())?;
        Self::buffer(left, &mut left_sink).await?;
        let mut right_sink = Buffer::make_sink(&mut self.buffer, right.schema())?;
        Self::buffer(right, &mut right_sink).await?;

        let mut left_source = Buffer::make_source(&mut self.buffer, left_sink).await?;
        let mut right_source = Buffer::make_source(&mut self.buffer, right_sink).await?;

        let left_input = left_source.unpartitioned().await?;
        let right_input = right_source.unpartitioned().await?;

        Self::assemble_and_produce(
            &mut self,
            Box::new([left_input, right_input]),
            output,
        )
        .await?;

        Ok(())
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
