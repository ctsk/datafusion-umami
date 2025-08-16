//! I/O using the the [`crate::spill`] module

use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;

use crate::spill::in_progress_spill_file::InProgressSpillFile;

impl super::BatchWriter for InProgressSpillFile {
    fn write(&mut self, batch: &arrow::array::RecordBatch) -> Result<()> {
        self.append_batch(batch)
    }

    fn finish(&mut self) -> Result<Option<RefCountedTempFile>> {
        InProgressSpillFile::finish(self)
    }
}
