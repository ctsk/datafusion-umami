use std::{prelude::rust_2024::Future, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{disk_manager::RefCountedTempFile, SendableRecordBatchStream};
use futures::Stream;

use crate::{
    common::collect, spill::in_progress_spill_file::InProgressSpillFile,
    EmptyRecordBatchStream, SpillManager,
};

use super::LazyPartitionBuffer;

const NAME: &str = "UMAMI_SPILL";

pub struct SpillBuffer {
    manager: Arc<SpillManager>,
}

impl SpillBuffer {
    pub fn new(manager: SpillManager) -> Self {
        Self {
            manager: Arc::new(manager),
        }
    }
}

impl LazyPartitionBuffer for SpillBuffer {
    type Sink = SpillSink;
    type Source = SpillSource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        let writer = self.manager.create_in_progress_file(NAME)?;
        Ok(Self::Sink { schema, writer })
    }

    async fn make_source(&mut self, mut sink: Self::Sink) -> Result<Self::Source> {
        let schema = sink.schema;
        let manager = Arc::clone(&self.manager);
        let spill_file = sink.writer.finish()?;
        Ok(Self::Source {
            schema,
            manager,
            spill_file,
        })
    }

    fn partition_count(&self) -> usize {
        0
    }
}

pub struct SpillSink {
    schema: SchemaRef,
    writer: InProgressSpillFile,
}

impl super::Sink for SpillSink {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.append_batch(&batch)
    }
}

pub struct SpillSource {
    schema: SchemaRef,
    manager: Arc<SpillManager>,
    spill_file: Option<RefCountedTempFile>,
}

impl SpillSource {
    pub fn new(
        schema: SchemaRef,
        manager: Arc<SpillManager>,
        spill_file: Option<RefCountedTempFile>,
    ) -> Self {
        Self {
            schema,
            manager,
            spill_file,
        }
    }
}

impl super::LazyPartitionedSource for SpillSource {
    type PartitionedSource = super::empty::EmptySource;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        match self.spill_file.take() {
            Some(file) => self.manager.read_spill_as_stream(file),
            None => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.schema,
            )))),
        }
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        super::empty::EmptySource::new(self.schema)
    }
}
