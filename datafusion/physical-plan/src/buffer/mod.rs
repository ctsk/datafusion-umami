use arrow_array::RecordBatch;
use datafusion_execution::disk_manager::RefCountedTempFile;

use crate::metrics;

pub(crate) mod adaptive_buffer;
pub(crate) mod memory_buffer;

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct PartitionMetrics {
    num_batches: metrics::Gauge,
    num_rows: metrics::Gauge,
    mem_used: metrics::Gauge,
}

impl PartitionMetrics {
    fn update(&mut self, batch: &RecordBatch) {
        self.num_batches.add(1);
        self.num_rows.add(batch.num_rows());
        self.mem_used.add(batch.get_array_memory_size());
    }
}

pub(crate) enum MaterializedPartition {
    InMemory {
        batches: Vec<RecordBatch>,
        metrics: PartitionMetrics,
    },
    Spilled {
        file: RefCountedTempFile,
        metrics: PartitionMetrics,
    },
}

impl MaterializedPartition {
    pub(crate) fn is_spilled(&self) -> bool {
        matches!(self, Self::Spilled { .. })
    }
}

pub(crate) struct MaterializedBatches {
    unpartitioned: Vec<RecordBatch>,
    partitions: Vec<MaterializedPartition>,
    total: PartitionMetrics,
    total_spilled: PartitionMetrics,
}

impl MaterializedBatches {
    pub(crate) fn from_unpartitioned(
        unpartitioned: Vec<RecordBatch>,
        total: PartitionMetrics,
    ) -> Self {
        Self {
            unpartitioned,
            partitions: vec![],
            total,
            total_spilled: PartitionMetrics::default(),
        }
    }

    pub(crate) fn from_partitioned(
        partitions: Vec<MaterializedPartition>,
        total: PartitionMetrics,
        total_spilled: PartitionMetrics,
    ) -> Self {
        Self {
            unpartitioned: vec![],
            partitions,
            total,
            total_spilled,
        }
    }

    pub(crate) fn from_partially_partitioned(
        unpartitioned: Vec<RecordBatch>,
        partitions: Vec<MaterializedPartition>,
        total: PartitionMetrics,
        total_spilled: PartitionMetrics,
    ) -> Self {
        Self {
            unpartitioned,
            partitions,
            total,
            total_spilled,
        }
    }

    pub(crate) fn mem_used(&self) -> usize {
        self.total.mem_used.value()
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.total.num_rows.value()
    }

    pub(crate) fn num_batches(&self) -> usize {
        self.total.num_batches.value()
    }

    pub(crate) fn take_mem_batches(&mut self) -> Vec<RecordBatch> {
        self.unpartitioned
            .drain(..)
            .chain(self.partitions.iter_mut().flat_map(|part| match part {
                MaterializedPartition::InMemory {
                    batches,
                    metrics: _,
                } => std::mem::take(batches),
                MaterializedPartition::Spilled {
                    file: _,
                    metrics: _,
                } => {
                    vec![]
                }
            }))
            .collect()
    }
}
