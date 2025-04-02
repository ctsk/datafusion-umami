use std::{
    collections::VecDeque, pin::Pin, sync::Arc, task::{ready, Context, Poll}
};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::error::Result;
use datafusion_execution::{
    disk_manager::RefCountedTempFile, runtime_env::RuntimeEnv, RecordBatchStream, SendableRecordBatchStream
};
use datafusion_physical_expr::{Partitioning, PhysicalExprRef};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

use crate::{
    buffer::{
        adaptive_buffer::{AdaptiveBuffer, AdaptiveBufferOptions},
        buffer_metrics::BufferMetrics,
        MaterializedBatches, MaterializedPartition, PartitionMetrics,
    }, common::IPCWriter, memory::MemoryStream, metrics, repartition::BatchPartitioner, stream::RecordBatchStreamAdapter
};

pub trait StreamFactory {
    fn schema(&self) -> SchemaRef;
    fn make(
        &self,
        input: Vec<Box<dyn FnOnce() -> SendableRecordBatchStream + Send>>,
    ) -> Result<SendableRecordBatchStream>;
}

pub(crate) struct AdaptiveMaterializeStream {
    stream_factory: Box<dyn StreamFactory + Send>,
    protos: VecDeque<InputProto>,
    metrics: BufferMetrics,
    runtime: Arc<RuntimeEnv>,
    options: AdaptiveBufferOptions,
}

type Sink = genawaiter::sync::Co<Result<RecordBatch>>;

pub(crate) struct InputProto {
    input: SendableRecordBatchStream,
    expr: Vec<PhysicalExprRef>,
}

impl InputProto {
    pub(crate) fn new(
        input: SendableRecordBatchStream,
        expr: Vec<PhysicalExprRef>,
    ) -> Self {
        Self {
            input,
            expr,
        }
    }
}

impl AdaptiveMaterializeStream {
    pub fn new(
        stream_factory: Box<dyn StreamFactory + Send>,
        protos: VecDeque<InputProto>,
        metrics: BufferMetrics,
        runtime: Arc<RuntimeEnv>,
        options: AdaptiveBufferOptions,
    ) -> Self {
        Self {
            stream_factory,
            protos,
            metrics,
            runtime,
            options,
        }
    }

    pub fn stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.stream_factory.schema(),
            genawaiter::sync::Gen::new(|co| self.run_wrapped(co)),
        ))
    }

    pub async fn run_wrapped(self, mut sink: Sink) {
        let run_result = self.run(&mut sink).await;

        if let Err(e) = run_result {
            let _ = sink.yield_(Err(e));
        }
    }

    pub fn new_buffer(&self, proto: &InputProto) -> Result<AdaptiveBuffer> {
        AdaptiveBuffer::try_new(
            proto.expr.clone(),
            self.runtime.clone(),
            proto.input.schema(),
            Some(self.options.clone()),
        )
    }

    pub async fn buffer_blocking(&mut self, mut blocking: InputProto) -> Result<MaterializedBatches> {
        let mut buffer = self.new_buffer(&blocking)?;
        while let Some(batch) = blocking.input.next().await {
            let batch = batch?;
            buffer.push(batch)?;
        }
        
        buffer.finalize(self.metrics.clone())
    }

    pub async fn produce(mut stream: SendableRecordBatchStream, sink: &mut Sink) -> Result<()>{
        while let Some(batch) = stream.next().await {
            sink.yield_(Ok(batch?)).await;
        }

        Ok(())
    }

    pub async fn run(mut self, sink: &mut Sink) -> Result<()> {
        let blocking = self.protos.pop_front().unwrap();

        // 1. Buffer the entire build side inside AdaptiveBuffer
        let mut materialized = self.buffer_blocking(blocking).await?;
        let spill_mask = materialized.spill_mask();

        let spill_mask = match spill_mask {
            None => {
                // No spilling occured => We only need to process memory batches.
                // The other streams (if exist) do not need to partition.

                let mem_batches = materialized.take_mem_batches();
                let mem_stream = MemoryStream::try_new(mem_batches, materialized.schema(), None)?;
                let producer: Box<dyn FnOnce() -> SendableRecordBatchStream + Send> = Box::new(move || Box::pin(mem_stream));
                let mut inputs: Vec<Box<dyn FnOnce() -> Pin<Box<dyn RecordBatchStream + Send>> + Send>> = vec![producer];

                for proto in self.protos.drain(..) {
                    inputs.push(Box::new(move || proto.input));
                }

                let stream: Pin<Box<dyn RecordBatchStream + Send>> = self.stream_factory.make(inputs)?;
                return Self::produce(stream, sink).await;
            }

            Some(spill_mask) => {
                spill_mask
            }
        };

        // 2. Construct the build side stream for in-memory batches
        
        let mem_batches = materialized.take_mem_batches();
        let mem_stream = MemoryStream::try_new(mem_batches, materialized.schema(), None)?;
        let producer: Box<dyn FnOnce() -> SendableRecordBatchStream + Send> = Box::new(move || Box::pin(mem_stream));

        let mut inputs: Vec<Box<dyn FnOnce() -> Pin<Box<dyn RecordBatchStream + Send>> + Send>> = vec![producer];

        // 3. Construct the probe side stream

        let mut stream_materialized = Vec::new();
        for proto in self.protos.drain(..) {
            let dummy_stream_materialized = MaterializedBatches::from_unpartitioned(
                proto.input.schema(),
                Vec::new(),
                PartitionMetrics::default()
            );
            let partitioned_stream_out: Arc<Mutex<MaterializedBatches>> = Arc::new(Mutex::new(dummy_stream_materialized));
            stream_materialized.push(Arc::clone(&partitioned_stream_out));
            let partitioned_stream = PartitioningFilter::try_new(
                proto.expr,
                proto.input.schema(),
                spill_mask.clone(),
                &self.runtime,
                proto.input,
                Arc::clone(&partitioned_stream_out),
            )?;

            inputs.push(Box::new(move || Box::pin(partitioned_stream)));
        }

        let stream: Pin<Box<dyn RecordBatchStream + Send>> = self.stream_factory.make(inputs)?;
        Self::produce(stream, sink).await?;
        
        while let Some((part, batches)) = materialized.take_next_spilled() {
            let batches = batches?;

            let mem_stream = MemoryStream::try_new(batches, materialized.schema(), None)?;
            let producer: Box<dyn FnOnce() -> SendableRecordBatchStream + Send> =
                Box::new(move || Box::pin(mem_stream));       
            let mut inputs = vec![producer];

            for sm in stream_materialized.iter() {
                let stream = sm.lock().stream_spill(part).unwrap()?;
                inputs.push(
                    Box::new(
                        move || stream
                    )
                )
            }

            let stream = self.stream_factory.make(inputs)?;
            Self::produce(stream, sink).await?;
        }

        Ok(())
    }
}

struct PartitioningFilter {
    out: Arc<Mutex<MaterializedBatches>>,
    spiller: Vec<Option<(RefCountedTempFile, IPCWriter)>>,
    partitioner: BatchPartitioner,
    buffered: Vec<RecordBatch>,
    input: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl PartitioningFilter {
    fn try_new(
        expr: Vec<PhysicalExprRef>,
        schema: SchemaRef,
        mask: Vec<bool>,
        runtime: &RuntimeEnv,
        input: SendableRecordBatchStream,
        out: Arc<Mutex<MaterializedBatches>>,
    ) -> Result<Self> {
        let mut spiller: Vec<_> = mask.iter().map(|_| None).collect();
        for (slot, _) in spiller.iter_mut().zip(mask).filter(|v| v.1) {
            let file = runtime.disk_manager.create_tmp_file("probe_spill")?;
            let writer = IPCWriter::new(file.path(), &schema)?;
            *slot = Some((file, writer))
        }

        Ok(Self {
            schema,
            partitioner: BatchPartitioner::try_new(
                Partitioning::Hash(expr, spiller.len()),
                metrics::Time::new(), // TODO register this metric
            )?,
            spiller,
            buffered: vec![],
            input,
            out,
        })
    }
}

impl Stream for PartitioningFilter {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for PartitioningFilter {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl PartitioningFilter {
    fn poll_next_impl(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        match self.poll_next_impl_inner(cx) {
            Poll::Ready(None) => {
                let mut materialized_partitions = Vec::new();
                for spiller in self.spiller.drain(..) {
                    let partition = match spiller {
                        Some((file, mut writer)) => {
                            writer.finish()?;

                            MaterializedPartition::Spilled {
                                file,
                                _metrics: PartitionMetrics::default(),
                            }
                        }
                        None => Default::default(),
                    };

                    materialized_partitions.push(partition);
                }

                let materialized_batches = MaterializedBatches::from_partitioned(
                    self.schema.clone(),
                    materialized_partitions,
                    PartitionMetrics::default(),
                    PartitionMetrics::default(),
                );

                *self.out.lock() = materialized_batches;

                Poll::Ready(None)
            }
            v => v,
        }
    }
    fn poll_next_impl_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if let Some(batch) = self.buffered.pop() {
            return Poll::Ready(Some(Ok(batch)));
        }

        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                for item in self.partitioner.partition_iter(batch)? {
                    let (part, batch) = item?;

                    match &mut self.spiller[part] {
                        Some(spiller) => spiller.1.write(&batch)?,
                        None => self.buffered.push(batch),
                    }
                }

                Poll::Ready(self.buffered.pop().map(Ok))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}
