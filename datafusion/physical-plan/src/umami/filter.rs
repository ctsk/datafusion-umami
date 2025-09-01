use std::sync::{Arc, Mutex};

use arrow::{array::RecordBatch, compute::concat_batches};
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use futures::StreamExt;

use crate::{
    repartition::Partitioner, stream::RecordBatchStreamAdapter,
    umami::buffer::PartitionedSink, utils::RowExpr,
};

pub struct PartingFilter<Sink> {
    input: SendableRecordBatchStream,
    sink: Option<Sink>,
    partitioner: Partitioner,
    airlock: Airlock<Sink>,
    mask: Vec<bool>,
}

pub struct Airlock<T> {
    value: Arc<Mutex<Option<T>>>,
}

impl<T> Default for Airlock<T> {
    fn default() -> Self {
        Self {
            value: Arc::new(Mutex::new(None)),
        }
    }
}

impl<T> Clone for Airlock<T> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
        }
    }
}

impl<T> Airlock<T> {
    fn put(&self, value: T) {
        *self.value.lock().unwrap() = Some(value);
    }

    pub fn get(&self) -> T {
        self.value.lock().unwrap().take().unwrap()
    }
}

type Output = genawaiter::sync::Co<Result<RecordBatch>>;

impl<Sink: PartitionedSink + Send + 'static> PartingFilter<Sink> {
    pub fn new(
        input: SendableRecordBatchStream,
        key: RowExpr,
        seed: ahash::RandomState,
        num_partitions: usize,
        mask: Vec<bool>,
        sink: Sink,
    ) -> (Self, Airlock<Sink>) {
        let airlock = Airlock::default();
        (
            Self {
                input,
                partitioner: Partitioner::new(key, seed, num_partitions),
                airlock: Airlock::clone(&airlock),
                sink: Some(sink),
                mask,
            },
            airlock,
        )
    }

    pub fn stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            genawaiter::sync::Gen::new(|co| self.run_wrapped(co)),
        ))
    }

    async fn run_wrapped(mut self, mut output: Output) {
        let run_result = self.run(&mut output).await;

        if let Err(e) = run_result {
            log::debug!("Materialize encoutered err: {:?}", e);
            let _ = output.yield_(Err(e)).await;
        }
    }

    async fn run(&mut self, output: &mut Output) -> Result<()> {
        let mut sink = self.sink.take().unwrap();

        let mut passthrough = Vec::new();
        while let Some(batch) = self.input.next().await {
            let parts = self.partitioner.partition(&[batch?])?;
            for (i, (delay, batch)) in self.mask.iter().zip(parts.into_iter()).enumerate()
            {
                if *delay {
                    sink.push_to_part(batch, i).await?;
                } else {
                    passthrough.push(batch);
                }
            }
            let batch = concat_batches(&self.input.schema(), &passthrough)?;
            output.yield_(Ok(batch)).await;
            passthrough.clear();
        }

        self.airlock.put(sink);

        Ok(())
    }
}
