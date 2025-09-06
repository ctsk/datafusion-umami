use arrow::record_batch::RecordBatch;
use datafusion_common::Result;

pub(super) mod aligned_ipc;
pub mod pinned_writer;
mod pool;
pub mod spill;
#[cfg(test)]
mod tests;
pub mod uring;

pub trait BatchWriter {
    type Intermediate: Send;

    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<Self::Intermediate>;
}

pub trait AsyncBatchWriter {
    type Intermediate: Send;

    async fn write(&mut self, batch: RecordBatch, part: usize) -> Result<()>;
    async fn finish(&mut self) -> Result<Self::Intermediate>;
}

pub trait PartitionedBatchWriter {
    type Intermediate: Send;

    fn write(&mut self, batch: &RecordBatch, partition: usize) -> Result<()>;
    fn finish(&mut self) -> Result<Self::Intermediate>;
}

pub struct AutoPartitionedBatchWriter<BatchWriter> {
    inner: Vec<BatchWriter>,
}

impl<T: BatchWriter> PartitionedBatchWriter for AutoPartitionedBatchWriter<T> {
    type Intermediate = Vec<T::Intermediate>;

    fn write(&mut self, batch: &RecordBatch, partition: usize) -> Result<()> {
        self.inner[partition].write(batch)
    }

    fn finish(&mut self) -> Result<Self::Intermediate> {
        self.inner
            .iter_mut()
            .map(|writer| writer.finish())
            .collect()
    }
}
