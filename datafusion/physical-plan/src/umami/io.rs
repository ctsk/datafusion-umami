use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;

mod spill;

trait BatchWriter {
    fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<Option<RefCountedTempFile>>;
}
