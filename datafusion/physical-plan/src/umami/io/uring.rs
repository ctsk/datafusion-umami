mod reader;
mod writer;

const IO_URING_DEPTH: usize = 64;
const DIRECT_IO_ALIGNMENT: usize = 512;
const BATCH_UPPER_BOUND: usize = 1 << 30;
const WRITE_LOWER_BOUND: usize = 1 << 16;

pub use reader::Reader;
pub use writer::Writer;
