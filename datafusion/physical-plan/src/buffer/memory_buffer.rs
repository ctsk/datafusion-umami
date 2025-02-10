use std::slice::Iter;

use arrow::array::RecordBatch;
use datafusion_common::error::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use crate::metrics;

use super::{MaterializedBatches, PartitionMetrics};
pub(crate) struct MemoryBuffer {
    batches: Vec<RecordBatch>,
    reservation: MemoryReservation,
    metrics: PartitionMetrics,
}

impl MemoryBuffer {
    pub(crate) fn new(reservation: MemoryReservation) -> Self {
        MemoryBuffer {
            batches: Vec::new(),
            reservation,
            metrics: Default::default(),
        }
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let mem_size = batch.get_array_memory_size();

        self.reservation.try_grow(mem_size)?;
        self.metrics.update(&batch);
        self.batches.push(batch);

        Ok(())
    }

    pub(crate) fn finalize(self) -> MaterializedBatches {
        MaterializedBatches::from_unpartitioned(self.batches, self.metrics)
    }
}