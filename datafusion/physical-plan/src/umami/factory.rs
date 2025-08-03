//! StreamFactory instantiates its stream for given input streams

use datafusion_execution::{SendableRecordBatchStream, TaskContext};

pub trait StreamProvider {
    fn get(&mut self, id: usize) -> SendableRecordBatchStream;
}

pub trait StreamFactory {
    fn make(
        &mut self,
        inputs: &mut dyn StreamProvider,
        partition: usize,
        context: &TaskContext,
    ) -> SendableRecordBatchStream;
}
