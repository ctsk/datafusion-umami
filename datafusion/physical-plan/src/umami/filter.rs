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

        let chunk_size = self.mask.len().min(16);
        let mut batcher = Vec::new();
        let mut passthrough = Vec::new();
        while let Some(batch) = self.input.next().await {
            batcher.push(batch?);
            if batcher.len() >= chunk_size {
                let parts = self.partitioner.partition(&batcher)?;
                for (i, (delay, batch)) in
                    self.mask.iter().zip(parts.into_iter()).enumerate()
                {
                    if batch.num_rows() > 0 {
                        if *delay {
                            sink.push_to_part(batch, i).await?;
                        } else {
                            output.yield_(Ok(batch)).await;
                        }
                    }
                }
                batcher.clear();
            }
        }

        if !batcher.is_empty() {
            let parts = self.partitioner.partition(&batcher)?;
            for (i, (delay, batch)) in self.mask.iter().zip(parts.into_iter()).enumerate()
            {
                if batch.num_rows() > 0 {
                    if *delay {
                        sink.push_to_part(batch, i).await?;
                    } else {
                        passthrough.push(batch);
                    }
                }
            }

            // Iterate over passthrough, concatenate batches so that all batches that are emitted have at num_rows() > 1024;
            // yield those batches.
            let mut to_concat = Vec::new();
            let mut to_concat_rows = 0;
            while !passthrough.is_empty() {
                let batch = passthrough.pop().unwrap();
                to_concat_rows += batch.num_rows();
                to_concat.push(batch);
                if to_concat_rows >= 1024 {
                    let batch = concat_batches(&self.input.schema(), &to_concat)?;
                    to_concat.clear();
                    to_concat_rows = 0;
                    output.yield_(Ok(batch)).await;
                }
            }
            if !to_concat.is_empty() {
                let batch = concat_batches(&self.input.schema(), &to_concat)?;
                to_concat.clear();
                output.yield_(Ok(batch)).await;
            }
        }

        self.airlock.put(sink);

        Ok(())
    }
}
