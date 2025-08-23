use std::sync::Arc;

use ahash::RandomState;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_arrow_extra::utils;
use datafusion_common::Result;

use crate::{
    repartition::Partitioner, umami::buffer::LazyPartitionBuffer, utils::RowExpr,
};

#[derive(Clone)]
pub struct AdaptiveSinkConfig {
    pub partition_start: u64,
}

enum SinkState {
    Unpartition,
    Partition,
}

pub struct AdaptiveSink {
    config: AdaptiveSinkConfig,
    partitioner: Partitioner,
    state: SinkState,
    unpartitioned_size: u64,
    unpartitioned: Vec<RecordBatch>,
    partitioned_sizes: Vec<u64>,
    partitioned: Vec<Vec<RecordBatch>>,
}

impl AdaptiveSink {
    pub fn new(
        config: AdaptiveSinkConfig,
        expr: RowExpr,
        num_partitions: usize,
        random_state: RandomState,
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
            partitioned_sizes: vec![0; num_partitions],
            partitioned: vec![vec![]; num_partitions],
        }
    }

    pub fn new_random(
        config: AdaptiveSinkConfig,
        expr: RowExpr,
        num_partitions: usize,
    ) -> Self {
        Self::new(config, expr, num_partitions, RandomState::new())
    }

    fn should_partition(&self) -> bool {
        self.unpartitioned_size >= self.config.partition_start
    }
}

impl super::Sink for AdaptiveSink {
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
                for (p, batch) in self
                    .partitioner
                    .partition(&[batch])?
                    .into_iter()
                    .enumerate()
                {
                    if batch.num_rows() > 0 {
                        self.partitioned_sizes[p] +=
                            utils::batch_size_shared(&batch) as u64;
                        self.partitioned[p].push(batch);
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(bon::Builder)]
pub struct AdaptiveBuffer {
    schema: SchemaRef,
    sink_config: AdaptiveSinkConfig,
    key_expr: RowExpr,
    num_partitions: usize,
}

impl LazyPartitionBuffer for AdaptiveBuffer {
    type Sink = AdaptiveSink;
    type Source = super::memory::MemorySource;

    fn make_sink(&mut self, _schema: SchemaRef) -> Result<Self::Sink> {
        Ok(Self::Sink::new_random(
            self.sink_config.clone(),
            self.key_expr.clone(),
            self.num_partitions,
        ))
    }

    async fn make_source(&mut self, sink: Self::Sink) -> Result<Self::Source> {
        Ok(super::memory::MemorySource::new(
            Arc::clone(&self.schema),
            sink.unpartitioned,
            sink.partitioned,
        ))
    }

    fn partition_count(&self) -> usize {
        self.num_partitions
    }
}
