//! A stripped-down version of the Arrow IPC format
//!
//! - Without a header
//! - With each batch aligned to a 512 byte boundary

use std::path::PathBuf;

use arrow::ipc::Block as IpcBlock;
use arrow_schema::SchemaRef;

#[derive(Debug)]
pub struct AlignedPartitionedIPC {
    pub schema: SchemaRef,
    pub path: PathBuf,
    pub blocks: Vec<(Vec<Loc>, Vec<BatchBlocks>)>,
}

#[derive(Clone, Debug)]
pub struct Loc {
    pub batch_blocks_offset: usize,
    pub file_offset: usize,
    pub length: usize,
}

#[derive(Clone, Debug)]
pub struct Block {
    pub meta_length: usize,
    pub data_length: usize,
    pub offset: usize,
}

impl Block {
    pub fn to_ipc(&self) -> IpcBlock {
        IpcBlock::new(
            self.offset as i64,
            self.meta_length as i32,
            self.data_length as i64,
        )
    }

    pub fn length(&self) -> usize {
        self.meta_length + self.data_length
    }
}

#[derive(Clone, Debug)]
pub struct BatchBlocks {
    pub dicts: Vec<Block>,
    pub batch: Block,
}

#[derive(Debug)]
pub struct BatchBlockWithPart(pub BatchBlocks, pub usize);
