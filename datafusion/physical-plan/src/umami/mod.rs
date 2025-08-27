#![allow(dead_code)]

mod buffer;
mod factory;
mod io;
#[cfg(test)]
mod tests;
mod wrapper;

use datafusion_common::Result;
use std::sync::Arc;

pub use buffer::AsyncSpillBuffer;
pub use buffer::IoUringSpillBuffer;
pub use buffer::SpillBuffer;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::TaskContext;
pub use factory::BasicStreamProvider;
pub use factory::StreamFactory;
pub use factory::StreamProvider;
use io::spill::InProgressSpillFileWithParts;
pub use wrapper::DefaultMaterializeWrapper;
pub use wrapper::InputKind;
pub use wrapper::MaterializeWrapper;

pub fn apply(
    factory: Box<dyn StreamFactory + Send>,
    input: InputKind,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let buffer = IoUringSpillBuffer::new(
        context.runtime_env(),
        context.session_config().options().x.recycle,
    );
    Ok(MaterializeWrapper::new(factory, input, partition, context, buffer).stream())
}
