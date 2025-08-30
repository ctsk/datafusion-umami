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

use crate::umami::buffer::AdaptiveBuffer;
use crate::umami::buffer::StaticHybridSinkConfig;

pub fn apply(
    factory: Box<dyn StreamFactory + Send>,
    input: InputKind,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let buffer = IoUringSpillBuffer::new(
        context.runtime_env(),
        context.session_config().options().x.recycle,
        context.session_config().options().x.direct_io,
        16,
    );
    let buffer = AdaptiveBuffer::builder()
        .num_partitions(16)
        .delegate(buffer)
        .sink_config(StaticHybridSinkConfig {
            partition_start: 1 << 24,
            delegate_start: 5 << 24,
        })
        .build();
    Ok(MaterializeWrapper::new(factory, input, partition, context, buffer).stream())
}
