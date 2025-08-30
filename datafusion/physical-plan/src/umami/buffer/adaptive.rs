use std::sync::Arc;

use ahash::RandomState;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_arrow_extra::utils;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

use crate::{
    memory::MemoryStream,
    repartition::Partitioner,
    umami::buffer::{
        LazyPartitionBuffer, PartitionBuffer, PartitionedSink, PartitionedSource,
    },
    utils::RowExpr,
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PartState {
    Owned,
    Delegated,
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
    pub fn new(
        config: StaticHybridSinkConfig,
        expr: RowExpr,
        num_partitions: usize,
        random_state: RandomState,
        inner: Inner,
        schema: SchemaRef,
    ) -> Self {
        let state = if config.partition_start > 0 {
            SinkState::Unpartition
        } else {
            SinkState::Partition
        };

        Self {
            config,
            partitioner: Partitioner::new(expr, random_state, num_partitions),
            state,
            unpartitioned_size: 0,
            unpartitioned: vec![],
            partitioned_total: 0,
            partitioned_sizes: vec![0; num_partitions],
            partitioned_state: vec![PartState::Owned; num_partitions],
            partitioned: vec![vec![]; num_partitions],
            inner,
            schema,
        }
    }

    pub fn new_random(
        config: StaticHybridSinkConfig,
        expr: RowExpr,
        num_partitions: usize,
        inner: Inner,
        schema: SchemaRef,
    ) -> Self {
        Self::new(
            config,
            expr,
            num_partitions,
            RandomState::new(),
            inner,
            schema,
        )
    }

    fn should_partition(&self) -> bool {
        self.unpartitioned_size > self.config.partition_start
    }

    fn should_evict(&self) -> bool {
        self.unpartitioned_size + self.partitioned_total > self.config.delegate_start
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

impl<Inner: PartitionedSink + Send> super::Sink for AdaptiveSink<Inner> {
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
                let parts = self.partitioner.partition(&self.unpartitioned[start..])?;
                for batch in self.unpartitioned.drain(start..) {
                    self.unpartitioned_size -= utils::batch_size_shared(&batch) as u64;
                }
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
        index: super::PartitionIdx,
    ) -> Result<SendableRecordBatchStream> {
        match self.partitioned_state[index.0] {
            PartState::Owned => Ok(Box::pin(MemoryStream::try_new(
                std::mem::take(&mut self.partitioned[index.0]),
                Arc::clone(&self.schema),
                None,
            )?)),
            PartState::Delegated => self.delegate.stream_partition(index).await,
        }
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
}

#[derive(bon::Builder)]
pub struct AdaptiveBuffer<Inner: PartitionBuffer + Send> {
    sink_config: StaticHybridSinkConfig,
    num_partitions: usize,
    delegate: Inner,
}

impl<Inner: PartitionBuffer + Send> LazyPartitionBuffer for AdaptiveBuffer<Inner> {
    type Sink = AdaptiveSink<Inner::Sink>;
    type Source = AdaptiveSource<Inner::Source>;

    fn make_sink(&mut self, schema: SchemaRef, key: RowExpr) -> Result<Self::Sink> {
        assert_eq!(self.partition_count(), self.delegate.partition_count());
        let inner = self.delegate.make_sink(Arc::clone(&schema))?;
        Ok(Self::Sink::new_random(
            self.sink_config.clone(),
            key,
            self.num_partitions,
            inner,
            schema,
        ))
    }

    async fn make_source(&mut self, sink: Self::Sink) -> Result<Self::Source> {
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
