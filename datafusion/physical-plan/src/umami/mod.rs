#![allow(dead_code)]

mod buffer;
mod factory;
mod filter;
mod io;
mod report;
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
    let opts = &context.session_config().options().x;
    let tps = context.session_config().target_partitions() as u64;
    let buffer = IoUringSpillBuffer::new(context.runtime_env(), opts);
    let buffer = AdaptiveBuffer::builder()
        .num_partitions(opts.part_count)
        .delegate(buffer)
        .sink_config(StaticHybridSinkConfig {
            partition_start: opts.partition_start as u64 / tps,
            delegate_start: opts.spill_start as u64 / tps,
        })
        .build();
    Ok(MaterializeWrapper::new(factory, input, partition, context, buffer).stream())
}
