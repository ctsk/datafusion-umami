use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;

use crate::umami::buffer::LazyPartitionBuffer;
use crate::umami::buffer::SinglePartAdapter;
use crate::umami::io::pinned_writer::make_pinned;
use crate::umami::io::pinned_writer::PinnedHandle;
use crate::umami::{io::AsyncBatchWriter, InProgressSpillFileWithParts};
use crate::utils::RowExpr;
use crate::SpillManager;

const NAME: &str = "UMAMI_ASYNC_SPILL";

pub struct SpillSink {
    schema: SchemaRef,
    writer: PinnedHandle<InProgressSpillFileWithParts>,
}

impl super::PartitionedSink for SpillSink {
    async fn push_to_part(&mut self, batch: RecordBatch, part: usize) -> Result<()> {
        self.writer.write(batch, part).await
    }
}

pub struct AsyncSpillBuffer {
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
    type Sink = SinglePartAdapter<SpillSink>;
    type Source = super::spill::SpillSource;

    fn make_sink(&mut self, schema: SchemaRef, _key: RowExpr) -> Result<Self::Sink> {
        let ipsf = self.manager.create_in_progress_file(NAME)?;
        let ipsfwp = InProgressSpillFileWithParts::new(ipsf);
        let writer = make_pinned(|| ipsfwp);
        Ok(Self::Sink {
            inner: SpillSink { schema, writer },
        })
    }

    async fn make_source(&mut self, mut sink: Self::Sink) -> Result<Self::Source> {
        let schema = sink.inner.schema;
        let manager = Arc::clone(&self.manager);
        let spill_file = sink.inner.writer.finish().await?.map(|f| f.file);
        Ok(super::spill::SpillSource::new(schema, manager, spill_file))
    }

    fn partition_count(&self) -> usize {
        0
    }
}
