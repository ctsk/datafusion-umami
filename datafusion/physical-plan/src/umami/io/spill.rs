//! I/O using the the [`crate::spill`] module

use datafusion_common::Result;
use datafusion_execution::disk_manager::RefCountedTempFile;

use crate::spill::in_progress_spill_file::InProgressSpillFile;

use super::BatchWriter;
use super::PartitionedBatchWriter;

impl BatchWriter for InProgressSpillFile {
    type Intermediate = Option<RefCountedTempFile>;

    fn write(&mut self, batch: &arrow::array::RecordBatch) -> Result<()> {
        self.append_batch(batch)
    }

    fn finish(&mut self) -> Result<Option<RefCountedTempFile>> {
        InProgressSpillFile::finish(self)
    }
}

pub struct PartitionedIPCFile {
    pub file: RefCountedTempFile,
    pub partition: Vec<usize>,
}

pub struct InProgressSpillFileWithParts {
    file: InProgressSpillFile,
    // Batch i belongs to partition partition[i]
    partition: Vec<usize>,
}

impl InProgressSpillFileWithParts {
    pub fn new(file: InProgressSpillFile) -> Self {
        Self {
            file,
            partition: vec![],
        }
    }
}

impl PartitionedBatchWriter for InProgressSpillFileWithParts {
    type Intermediate = Option<PartitionedIPCFile>;

    fn write(
        &mut self,
        batch: &arrow::array::RecordBatch,
        partition: usize,
    ) -> Result<()> {
        self.file.write(batch)?;
        self.partition.push(partition);
        Ok(())
    }

    fn finish(&mut self) -> Result<Self::Intermediate> {
        Ok(
            InProgressSpillFile::finish(&mut self.file)?.map(|file| PartitionedIPCFile {
                file,
                partition: std::mem::take(&mut self.partition),
            }),
        )
    }
}
