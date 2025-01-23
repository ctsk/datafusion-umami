use std::slice::Iter;

use arrow::array::RecordBatch;
use datafusion_common::error::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use crate::metrics;
pub(crate) struct MemoryBuffer {
    batches: Vec<RecordBatch>,
    reservation: MemoryReservation,
    mem_used: metrics::Gauge,
    num_rows: metrics::Gauge,
}

impl MemoryBuffer {
    pub(crate) fn new(reservation: MemoryReservation) -> Self {
        MemoryBuffer {
            batches: Vec::new(),
            reservation,
            mem_used: Default::default(),
            num_rows: Default::default(),
        }
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let mem_size = batch.get_array_memory_size();

        self.reservation.try_grow(mem_size)?;
        self.num_rows.add(batch.num_rows());
        self.mem_used.add(mem_size);
        self.batches.push(batch);

        Ok(())
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.num_rows.value()
    }

    pub(crate) fn mem_used(&self) -> usize {
        self.mem_used.value()
    }

    pub(crate) fn num_batches(&self) -> usize {
        self.batches.len()
    }

    pub(crate) fn iter(&self) -> Iter<RecordBatch> {
        self.batches.iter()
    }
}