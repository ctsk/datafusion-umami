use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;

use crate::umami::buffer::LazyPartitionBuffer;
use crate::umami::io::pinned_writer::make_pinned;
use crate::umami::io::pinned_writer::PinnedHandle;
use crate::umami::{io::AsyncBatchWriter, InProgressSpillFileWithParts};
use crate::SpillManager;

const NAME: &str = "UMAMI_ASYNC_SPILL";

pub struct SpillSink {
    schema: SchemaRef,
    writer: PinnedHandle<InProgressSpillFileWithParts>,
}

impl super::Sink for SpillSink {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(batch, 0).await
    }
}

struct AsyncSpillBuffer {
    manager: Arc<SpillManager>,
}

impl AsyncSpillBuffer {
    pub fn new(manager: SpillManager) -> Self {
        Self {
            manager: Arc::new(manager),
        }
    }
}

impl LazyPartitionBuffer for AsyncSpillBuffer {
    type Sink = SpillSink;
    type Source = super::spill::SpillSource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        let ipsf = self.manager.create_in_progress_file(NAME)?;
        let ipsfwp = InProgressSpillFileWithParts::new(ipsf);
        let writer = make_pinned(ipsfwp);
        Ok(Self::Sink { schema, writer })
    }

    async fn make_source(&mut self, mut sink: Self::Sink) -> Result<Self::Source> {
        let schema = sink.schema;
        let manager = Arc::clone(&self.manager);
        let spill_file = sink.writer.finish().await?.map(|f| f.file);
        Ok(super::spill::SpillSource::new(schema, manager, spill_file))
    }

    fn partition_count(&self) -> usize {
        0
    }
}
