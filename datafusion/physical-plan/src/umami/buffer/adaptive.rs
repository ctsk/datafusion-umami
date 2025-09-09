use std::{ops::Range, sync::Arc};

use ahash::RandomState;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_arrow_extra::utils;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

use crate::{
    memory::MemoryStream,
    repartition::Partitioner,
    umami::{
        buffer::{
            LazyPartitionBuffer, PartState, PartitionBuffer, PartitionIdx,
            PartitionedSink, PartitionedSource, Sink,
        },
        filter,
        report::BufferReport,
    },
    utils::{CoalesceStream, RowExpr},
};

#[derive(Clone)]
pub struct StaticHybridSinkConfig {
    pub partition_start: u64,
    pub delegate_start: u64,
}

enum SinkState {
    Unpartition,
    Partition,
}

pub struct AdaptiveSink<Inner> {
    config: StaticHybridSinkConfig,
    partitioner: Partitioner,
    state: SinkState,
    unpartitioned_size: u64,
    unpartitioned: Vec<RecordBatch>,
    partitioned_total: u64,
    partitioned_sizes: Vec<u64>,
    partitioned_state: Vec<PartState>,
    partitioned: Vec<Vec<RecordBatch>>,
    inner: Inner,
    schema: SchemaRef,
}

impl<Inner> AdaptiveSink<Inner> {
    fn new(
        config: StaticHybridSinkConfig,
        expr: RowExpr,
        num_partitions: usize,
        random_state: RandomState,
        inner: Inner,
        schema: SchemaRef,
        mask: Option<Vec<PartState>>,
    ) -> Self {
        let state = if config.partition_start > 0 && mask.is_none() {
            SinkState::Unpartition
        } else {
            SinkState::Partition
        };

        let mask = mask.unwrap_or_else(|| vec![PartState::Owned; num_partitions]);

        Self {
            config,
            partitioner: Partitioner::new(expr, random_state, num_partitions),
            state,
            unpartitioned_size: 0,
            unpartitioned: vec![],
            partitioned_total: 0,
            partitioned_sizes: vec![0; num_partitions],
            partitioned_state: mask,
            partitioned: vec![vec![]; num_partitions],
            inner,
            schema,
        }
    }

    fn should_partition(&self) -> bool {
        self.unpartitioned_size > self.config.partition_start
    }

    fn should_evict(&self) -> bool {
        self.partitioned_total > self.config.delegate_start
    }

    fn pick_eviction(&self) -> Option<usize> {
        self.partitioned_sizes
            .iter()
            .zip(self.partitioned_state.iter())
            .enumerate()
            .filter(|(_, (_, state))| **state == PartState::Owned)
            .max_by_key(|(_, (size, _))| **size)
            .map(|v| v.0)
    }

    fn num_partitions(&self) -> usize {
        self.partitioned_state.len()
    }
}

impl<Inner: PartitionedSink + Send> PartitionedSink for AdaptiveSink<Inner> {
    async fn push_to_part(&mut self, batch: RecordBatch, part: usize) -> Result<()> {
        match self.partitioned_state[part] {
            PartState::Owned => {
                self.partitioned_sizes[part] += utils::batch_size_shared(&batch) as u64;
                self.partitioned[part].push(batch);
                Ok(())
            }
            PartState::Delegated => self.inner.push_to_part(batch, part).await,
        }
    }
}

impl<Inner: PartitionedSink + Send> AdaptiveSink<Inner> {
    async fn partition_and_push(&mut self, range: Range<usize>) -> Result<()> {
        let parts = self.partitioner.partition(&self.unpartitioned[range])?;

        for (p, batch) in parts.into_iter().enumerate() {
            if batch.num_rows() > 0 {
                match self.partitioned_state[p] {
                    PartState::Owned => {
                        let size = utils::batch_size_shared(&batch) as u64;
                        self.partitioned_sizes[p] += size;
                        self.partitioned_total += size;
                        self.partitioned[p].push(batch);
                    }
                    PartState::Delegated => {
                        self.inner.push_to_part(batch, p).await?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl<Inner: PartitionedSink + Send> Sink for AdaptiveSink<Inner> {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        match self.state {
            SinkState::Unpartition => {
                self.unpartitioned_size += utils::batch_size_shared(&batch) as u64;
                self.unpartitioned.push(batch);

                if self.should_partition() {
                    self.state = SinkState::Partition;
                }
            }
            SinkState::Partition => {
                self.unpartitioned_size += utils::batch_size_shared(&batch) as u64;
                self.unpartitioned.push(batch);
                // Partition `target_chunk_size` batches at once to avoid producing too small output batches
                let target_chunk_size = self.num_partitions().min(32);
                let start = self.unpartitioned.len().saturating_sub(target_chunk_size);
                let range = start..self.unpartitioned.len();
                self.partition_and_push(range.clone()).await?;

                for batch in self.unpartitioned.drain(range) {
                    self.unpartitioned_size -= utils::batch_size_shared(&batch) as u64;
                }

                while self.should_evict() {
                    let Some(eviction) = self.pick_eviction() else {
                        panic!("OOM reached with no evictable partition")
                    };

                    for batch in self.partitioned[eviction].drain(..) {
                        self.inner.push_to_part(batch, eviction).await?;
                    }

                    self.partitioned_total -= self.partitioned_sizes[eviction];
                    self.partitioned_sizes[eviction] = 0;
                    self.partitioned_state[eviction] = PartState::Delegated;
                }
            }
        }

        Ok(())
    }

    async fn force_partition(&mut self) -> Result<()> {
        let len = self.unpartitioned.len();
        let chunk_size = self.num_partitions().min(32);

        for start in (0..len).step_by(chunk_size) {
            let end = (start + chunk_size).min(len);
            self.partition_and_push(start..end).await?;
        }

        self.unpartitioned.clear();
        self.unpartitioned_size = 0;
        Ok(())
    }
}

pub struct AdaptiveSource<Inner> {
    schema: SchemaRef,
    unpartitioned: Vec<RecordBatch>,
    partitioned_state: Vec<PartState>,
    partitioned: Vec<Vec<RecordBatch>>,
    delegate: Inner,
}

impl<Inner: PartitionedSource + Send> PartitionedSource for AdaptiveSource<Inner> {
    async fn stream_partition(
        &mut self,
        index: PartitionIdx,
    ) -> Result<SendableRecordBatchStream> {
        let stream = match self.partitioned_state[index.0] {
            PartState::Owned => Box::pin(MemoryStream::try_new(
                std::mem::take(&mut self.partitioned[index.0]),
                Arc::clone(&self.schema),
                None,
            )?),
            PartState::Delegated => self.delegate.stream_partition(index).await?,
        };

        Ok(CoalesceStream::new(stream, 896).stream())
    }

    fn partition_sequence(&self) -> impl Iterator<Item = usize> {
        let owned = |i: &usize| self.partitioned_state[*i] == PartState::Owned;
        let delegated = |i: &usize| self.partitioned_state[*i] == PartState::Delegated;
        Iterator::chain(
            (0..self.partition_count()).filter(owned),
            (0..self.partition_count()).filter(delegated),
        )
    }

    fn partition_count(&self) -> usize {
        self.partitioned_state.len()
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<Inner: PartitionedSource + Send> super::LazyPartitionedSource
    for AdaptiveSource<Inner>
{
    type PartitionedSource = Self;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            std::mem::take(&mut self.unpartitioned),
            Arc::clone(&self.schema),
            None,
        )?))
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        self
    }

    async fn all_in_mem(&mut self) -> Result<SendableRecordBatchStream> {
        let mut batches = std::mem::take(&mut self.unpartitioned);
        for (_, mut pbatches) in self
            .partitioned_state
            .iter()
            .zip(self.partitioned.iter_mut())
        {
            batches.append(&mut pbatches);
        }
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

#[derive(Clone)]
pub struct AdaptiveBuffer<Inner: PartitionBuffer + Send> {
    sink_config: StaticHybridSinkConfig,
    num_partitions: usize,
    delegate: Inner,
    seed: RandomState,
}

impl<Inner: PartitionBuffer + Send> AdaptiveBuffer<Inner> {
    pub fn new(
        sink_config: StaticHybridSinkConfig,
        num_partitions: usize,
        delegate: Inner,
    ) -> Self {
        Self {
            sink_config,
            num_partitions,
            delegate,
            seed: RandomState::new(),
        }
    }
}

impl<Inner: PartitionBuffer + Send + 'static> LazyPartitionBuffer
    for AdaptiveBuffer<Inner>
{
    type Sink = AdaptiveSink<Inner::Sink>;
    type Source = AdaptiveSource<Inner::Source>;

    fn make_sink(&mut self, schema: SchemaRef, key: RowExpr) -> Result<Self::Sink> {
        assert_eq!(self.partition_count(), self.delegate.partition_count());
        let inner = self.delegate.make_sink(Arc::clone(&schema))?;
        Ok(Self::Sink::new(
            self.sink_config.clone(),
            key,
            self.num_partitions,
            self.seed.clone(),
            inner,
            schema,
            None,
        ))
    }

    fn make_filtered_stream(
        &mut self,
        sink: &Self::Sink,
        stream: SendableRecordBatchStream,
        key: RowExpr,
    ) -> Result<(SendableRecordBatchStream, filter::Airlock<Self::Sink>)> {
        let inner = self.delegate.make_sink(Arc::clone(&stream.schema()))?;
        let sink = Self::Sink::new(
            self.sink_config.clone(),
            key.clone(),
            self.num_partitions,
            sink.partitioner.get_seed(),
            inner,
            stream.schema(),
            Some(sink.partitioned_state.clone()),
        );
        let (filter, airlock) = filter::PartingFilter::new(
            stream,
            key,
            sink.partitioner.get_seed(),
            sink.num_partitions(),
            sink.partitioned_state
                .iter()
                .map(|ps| *ps == PartState::Delegated)
                .collect(),
            sink,
        );
        Ok((filter.stream(), airlock))
    }

    fn probe_sink(&self, sink: &Self::Sink) -> BufferReport {
        let owned = |i: &usize| sink.partitioned_state[*i] == PartState::Owned;
        let delegated = |i: &usize| sink.partitioned_state[*i] == PartState::Delegated;
        let parts_in_mem = (0..sink.partitioned_state.len())
            .filter(owned)
            .map(PartitionIdx)
            .collect();
        let parts_oom = (0..sink.partitioned_state.len())
            .filter(delegated)
            .map(PartitionIdx)
            .collect();
        BufferReport {
            unpart_batches: sink.unpartitioned.len(),
            parts_in_mem,
            parts_oom,
        }
    }

    async fn make_source(&mut self, mut sink: Self::Sink) -> Result<Self::Source> {
        let delegated = |s: &PartState| *s == PartState::Delegated;
        if sink.partitioned_state.iter().any(delegated) {
            sink.force_partition().await?;
            assert!(sink.unpartitioned.is_empty());
        }

        Ok(AdaptiveSource {
            schema: sink.schema,
            unpartitioned: sink.unpartitioned,
            partitioned_state: sink.partitioned_state,
            partitioned: sink.partitioned,
            delegate: self.delegate.make_source(sink.inner).await?,
        })
    }

    fn partition_count(&self) -> usize {
        self.num_partitions
    }
}
