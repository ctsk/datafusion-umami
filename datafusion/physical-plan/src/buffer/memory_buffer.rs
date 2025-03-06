#![allow(dead_code)]

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::error::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use super::{MaterializedBatches, PartitionMetrics};
pub(crate) struct MemoryBuffer {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    reservation: MemoryReservation,
    metrics: PartitionMetrics,
}

impl MemoryBuffer {
    pub(crate) fn new(schema: SchemaRef, reservation: MemoryReservation) -> Self {
        MemoryBuffer {
            schema,
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
        MaterializedBatches::from_unpartitioned(self.schema, self.batches, self.metrics)
    }
}