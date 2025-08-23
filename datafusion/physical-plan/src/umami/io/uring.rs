mod reader;
mod writer;

const IO_URING_DEPTH: usize = 1;
const DIRECT_IO_ALIGNMENT: usize = 512;
const BATCH_UPPER_BOUND: usize = 1 << 30;
const WRITE_LOWER_BOUND: usize = 1 << 20;

use std::rc::Rc;

use io_uring_async::IoUringAsync;
pub use reader::Reader;
pub use writer::Writer;

struct RefSendWrapper {
    inner: Rc<IoUringAsync>,
}

impl RefSendWrapper {
    pub fn new(inner: Rc<IoUringAsync>) -> Self {
        Self { inner }
    }

    pub fn as_ref(&self) -> &IoUringAsync {
        &self.inner
    }
}

unsafe impl Send for RefSendWrapper {}
unsafe impl Sync for RefSendWrapper {}
