use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{
    disk_manager::RefCountedTempFile, runtime_env::RuntimeEnv, SendableRecordBatchStream,
};

use crate::{
    metrics::{ExecutionPlanMetricsSet, SpillMetrics},
    spill::in_progress_spill_file::InProgressSpillFile,
    umami::{
        buffer::{PartitionIdx, SinglePartAdapter},
        report::BufferReport,
    },
    utils::RowExpr,
    SpillManager,
};

use super::LazyPartitionBuffer;

const NAME: &str = "UMAMI_SPILL";

pub struct SpillBuffer {
    runtime: Arc<RuntimeEnv>,
    metrics: ExecutionPlanMetricsSet,
}

impl SpillBuffer {
    pub fn new(runtime: Arc<RuntimeEnv>, metrics: ExecutionPlanMetricsSet) -> Self {
        Self { runtime, metrics }
    }
}

impl LazyPartitionBuffer for SpillBuffer {
    type Sink = SinglePartAdapter<SpillSink>;
    type Source = SpillSource;

    fn make_sink(&mut self, schema: SchemaRef, _key: RowExpr) -> Result<Self::Sink> {
        let manager = SpillManager::new(
            Arc::clone(&self.runtime),
            SpillMetrics::new(&self.metrics, 0),
            Arc::clone(&schema),
        );
        let writer = manager.create_in_progress_file(NAME)?;
        Ok(SinglePartAdapter {
            inner: SpillSink {
                schema,
                manager: Arc::new(manager),
                writer,
            },
        })
    }

    async fn make_source(&mut self, mut sink: Self::Sink) -> Result<Self::Source> {
        let schema = sink.inner.schema;
        let manager = Arc::clone(&sink.inner.manager);
        let spill_file = sink.inner.writer.finish()?;
        Ok(Self::Source {
            schema,
            manager,
            spill_file,
        })
    }

    fn partition_count(&self) -> usize {
        1
    }

    fn probe_sink(&self, _sink: &Self::Sink) -> BufferReport {
        BufferReport {
            unpart_batches: 0,
            parts_in_mem: vec![],
            parts_oom: vec![PartitionIdx(0)],
        }
    }
}

pub struct SpillSink {
    schema: SchemaRef,
    manager: Arc<SpillManager>,
    writer: InProgressSpillFile,
}

impl super::PartitionedSink for SpillSink {
    async fn push_to_part(&mut self, batch: RecordBatch, partition: usize) -> Result<()> {
        assert!(partition == 0);
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
        todo!()
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        todo!()
    }

    async fn all_in_mem(&mut self) -> Result<SendableRecordBatchStream> {
        self.unpartitioned().await
    }
}
