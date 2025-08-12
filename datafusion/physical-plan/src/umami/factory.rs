//! StreamFactory instantiates its stream for given input streams

use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};

pub trait StreamProvider {
    fn get(&mut self, id: usize) -> SendableRecordBatchStream;
}

pub struct BasicStreamProvider {
    inputs: Vec<Option<SendableRecordBatchStream>>,
}

impl BasicStreamProvider {
    pub fn new(iter: impl IntoIterator<Item = SendableRecordBatchStream>) -> Self {
        Self {
            inputs: iter.into_iter().map(Some).collect(),
        }
    }
}

impl StreamProvider for BasicStreamProvider {
    fn get(&mut self, id: usize) -> SendableRecordBatchStream {
        self.inputs[id]
            .take()
            .expect("The stream was already taken.")
    }
}

pub trait StreamFactory {
    fn make(
        &mut self,
        inputs: &mut dyn StreamProvider,
        partition: usize,
        context: &TaskContext,
    ) -> Result<SendableRecordBatchStream>;
}
