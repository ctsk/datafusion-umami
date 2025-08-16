use std::{future::Future, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

use crate::{memory::MemoryStream, umami::buffer::LazyPartitionBuffer};

pub struct MemorySink {
    schema: SchemaRef,
    buffers: Vec<RecordBatch>,
}

pub struct MemorySource {
    schema: SchemaRef,
    buffers: Vec<RecordBatch>,
}

impl super::Sink for MemorySink {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.buffers.push(batch);
        Ok(())
    }
}

impl super::LazyPartitionedSource for MemorySource {
    type PartitionedSource = super::empty::EmptySource;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        let memory = MemoryStream::try_new(
            std::mem::take(&mut self.buffers),
            Arc::clone(&self.schema),
            None,
        )
        .expect("Failed to create memory stream");

        Ok(Box::pin(memory))
    }

    async fn into_partitioned(self) -> Self::PartitionedSource {
        assert!(self.buffers.is_empty());
        super::empty::EmptySource::new(self.schema)
    }
}

#[derive(Default)]
pub struct MemoryBuffer {}

impl LazyPartitionBuffer for MemoryBuffer {
    type Sink = MemorySink;
    type Source = MemorySource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        Ok(MemorySink {
            schema,
            buffers: vec![],
        })
    }

    fn make_source(&mut self, sink: Self::Sink) -> Result<Self::Source> {
        Ok(MemorySource {
            schema: sink.schema,
            buffers: sink.buffers,
        })
    }
}
