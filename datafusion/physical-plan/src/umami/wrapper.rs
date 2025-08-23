use std::sync::Arc;

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
use futures::StreamExt;

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
    Placeholder,
}

impl InputKind {
    pub fn unary(input: SendableRecordBatchStream, expr: Vec<PhysicalExprRef>) -> Self {
        Self::Unary { input, expr }
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
        input: SendableRecordBatchStream,
        output: &mut Output,
    ) -> Result<()> {
        let mut inputs = BasicStreamProvider::new([input]);
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
        Self::assemble_and_produce(&mut self, source.unpartitioned().await?, output)
            .await?;

        if self.buffer.partition_count() > 0 {
            let mut source = source.into_partitioned();
            for partition in 0..self.buffer.partition_count() {
                let stream = source.stream_partition(PartitionIdx(partition)).await;
                Self::assemble_and_produce(&mut self, stream, output).await?;
            }
        }
        Ok(())
    }
}
