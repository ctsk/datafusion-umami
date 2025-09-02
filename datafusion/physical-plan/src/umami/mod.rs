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
use std::panic;
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
    if opts.hard_disable {
        return execute_direct(input, factory, partition, context);
    }
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

fn execute_direct(
    input: InputKind,
    mut factory: Box<dyn StreamFactory + Send>,
    partition: usize,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    match input {
        InputKind::Unary { input, expr: _ } => {
            factory.make(&mut BasicStreamProvider::new([input]), partition, &context)
        }
        InputKind::Binary {
            left,
            left_expr: _,
            right,
            right_expr: _,
        } => factory.make(
            &mut BasicStreamProvider::new([left, right]),
            partition,
            &context,
        ),
        InputKind::Placeholder => {
            panic!("Placeholder input not supported")
        }
    }
}
