use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::error::Result;
use datafusion_execution::SendableRecordBatchStream;
use futures::StreamExt;

use crate::{
    buffer::{
        adaptive_buffer::AdaptiveBuffer, buffer_metrics::BufferMetrics,
    },
    memory::MemoryStream,
    stream::RecordBatchStreamAdapter,
};

pub(crate) trait StreamFactory {
    fn make(&self, input: SendableRecordBatchStream) -> Result<SendableRecordBatchStream>;
}

pub(crate) struct AdaptiveMaterializeStream {
    stream_factory: Box<dyn StreamFactory + Send>,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metrics: BufferMetrics,
}

pub(crate) fn materialize() -> AdaptiveMaterializeStream {
    todo!()
}

type Sink = genawaiter::sync::Co<Result<RecordBatch>>;

impl AdaptiveMaterializeStream {
    pub fn stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            genawaiter::sync::Gen::new(|co| self.run_wrapped(co)),
        ))
    }

    pub async fn run_wrapped(self, mut sink: Sink) {
        let run_result = self.run(&mut sink).await;

        if let Err(e) = run_result {
            let _ = sink.yield_(Err(e));
        }
    }

    pub fn mk_buffer(&self) -> AdaptiveBuffer {
        todo!()
    }

    pub async fn run(mut self, sink: &mut Sink) -> Result<()> {
        let mut buffer = self.mk_buffer();
        while let Some(batch) = self.input.next().await {
            let batch = batch?;
            buffer.push(batch)?;
        }

        let mut materialized = buffer.finalize(self.metrics.clone())?;
        let mem_batches = materialized.take_mem_batches();
        let mem_stream = MemoryStream::try_new(mem_batches, self.input.schema(), None)?;
        let mut stream = self.stream_factory.make(Box::pin(mem_stream))?;

        while let Some(batch) = stream.next().await {
            sink.yield_(Ok(batch?)).await;
        }

        while let Some((_part, batches)) = materialized.take_next_spilled() {
            let batches = batches?;
            let mem_stream = MemoryStream::try_new(batches, self.input.schema(), None)?;
            let mut stream = self.stream_factory.make(Box::pin(mem_stream))?;

            while let Some(batch) = stream.next().await {
                sink.yield_(Ok(batch?)).await
            }
        }

        Ok(())
    }
}