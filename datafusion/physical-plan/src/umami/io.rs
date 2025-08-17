use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;

mod spill;

trait BatchWriter {
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<Option<RefCountedTempFile>>;
}

trait PartitionedBatchWriter {
    fn write(&mut self, batch: &RecordBatch, partition: usize) -> Result<()>;
}

struct AutoPartitionedBatchWriter<BatchWriter> {
    inner: Vec<BatchWriter>,
}

impl<T: BatchWriter> PartitionedBatchWriter for AutoPartitionedBatchWriter<T> {
    fn write(&mut self, batch: &RecordBatch, partition: usize) -> Result<()> {
        self.inner[partition].write(batch)
    }
}
