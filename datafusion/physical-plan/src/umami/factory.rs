//! StreamFactory instantiates its stream for given input streams

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};

pub trait StreamProvider {
    fn get(&mut self, id: usize) -> SendableRecordBatchStream;
    fn estimate(&mut self, id: usize) -> Option<usize>;
}

pub struct BasicStreamProvider {
    inputs: Vec<Option<SendableRecordBatchStream>>,
    estimates: Vec<Option<usize>>,
}

impl BasicStreamProvider {
    pub fn new(iter: impl IntoIterator<Item = SendableRecordBatchStream>) -> Self {
        let inputs: Vec<_> = iter.into_iter().map(Some).collect();
        let estimates = vec![None; inputs.len()];
        Self { inputs, estimates }
    }

    pub fn new_with_estimates(
        iter: impl IntoIterator<Item = SendableRecordBatchStream>,
        estimates: impl IntoIterator<Item = Option<usize>>,
    ) -> Self {
        Self {
            inputs: iter.into_iter().map(Some).collect(),
            estimates: estimates.into_iter().collect(),
        }
    }
}

impl StreamProvider for BasicStreamProvider {
    fn get(&mut self, id: usize) -> SendableRecordBatchStream {
        self.inputs[id]
            .take()
            .expect("The stream was already taken.")
    }

    fn estimate(&mut self, id: usize) -> Option<usize> {
        self.estimates[id]
    }
}

pub trait StreamFactory {
    fn make(
        &mut self,
        inputs: &mut dyn StreamProvider,
        partition: usize,
        context: &TaskContext,
    ) -> Result<SendableRecordBatchStream>;

    fn output_schema(&self) -> SchemaRef;
}
