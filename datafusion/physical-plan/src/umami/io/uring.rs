mod reader_runtime;
mod writer;
mod writer_runtime;

// const IO_URING_DEPTH: usize = 8;
pub const DIRECT_IO_ALIGNMENT: usize = 512;
pub const BATCH_UPPER_BOUND: usize = 1 << 30;
// const WRITE_LOWER_BOUND: usize = 1 << 20;

use std::rc::Rc;

use datafusion_common::config::ExperimentalOptions;
use io_uring_async::IoUringAsync;

pub use reader_runtime::Reader;
pub use writer::Writer as RuntimeFreeWriter;
pub use writer_runtime::Writer as RuntimeWriter;

use crate::umami::io::AsyncBatchWriter;

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

#[derive(Clone)]
pub struct WriteOpts {
    pub direct_io: bool,
    pub ring_depth: usize,
    pub write_lower_bound: usize,
}

impl WriteOpts {
    pub fn from_config(x: &ExperimentalOptions) -> Self {
        Self {
            direct_io: x.direct_io_writer,
            ring_depth: x.uring_depth_writer,
            write_lower_bound: x.write_buffer_size,
        }
    }
}

impl Default for WriteOpts {
    fn default() -> Self {
        Self {
            direct_io: false,
            ring_depth: 16,
            write_lower_bound: 1 << 20,
        }
    }
}

#[derive(Clone)]
pub struct ReadOpts {
    direct_io: bool,
    ring_depth: usize,
    readahead: usize,
    recycle: bool,
}

impl ReadOpts {
    pub fn from_config(x: &ExperimentalOptions) -> Self {
        Self {
            direct_io: x.direct_io_reader,
            ring_depth: x.uring_depth_reader,
            readahead: x.readahead,
            recycle: x.recycle,
        }
    }
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            direct_io: false,
            ring_depth: 16,
            readahead: 16,
            recycle: false,
        }
    }
}

pub enum Writer {
    WithRuntime(RuntimeWriter),
    WithoutRuntime(RuntimeFreeWriter),
}

impl Writer {
    pub fn new_with_runtime(writer: RuntimeWriter) -> Self {
        Writer::WithRuntime(writer)
    }

    pub fn new_without_runtime(writer: RuntimeFreeWriter) -> Self {
        Writer::WithoutRuntime(writer)
    }
}

impl AsyncBatchWriter for Writer {
    type Intermediate = <writer::Writer as AsyncBatchWriter>::Intermediate;

    async fn write(
        &mut self,
        batch: arrow::array::RecordBatch,
        part: usize,
    ) -> datafusion_common::Result<()> {
        match self {
            Writer::WithRuntime(writer) => writer.write(batch, part).await,
            Writer::WithoutRuntime(writer) => writer.write(batch, part).await,
        }
    }

    async fn finish(&mut self) -> datafusion_common::Result<Self::Intermediate> {
        match self {
            Writer::WithRuntime(writer) => writer.finish().await,
            Writer::WithoutRuntime(writer) => writer.finish().await,
        }
    }
}
