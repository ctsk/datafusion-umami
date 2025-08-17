use std::{future::Future, sync::Arc};

use ahash::{random_state, RandomState};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::{hash_utils::create_hashes, Result};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_expr::PhysicalExprRef;

use crate::{
    memory::MemoryStream,
    repartition::Partitioner,
    umami::buffer::{LazyPartitionBuffer, PartitionedSource},
    utils::RowExpr,
};

pub struct MemorySink {
    schema: SchemaRef,
    buffers: Vec<RecordBatch>,
}

impl super::Sink for MemorySink {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.buffers.push(batch);
        Ok(())
    }
}

pub struct PartitionedMemorySink {
    partitioner: Partitioner,
    schema: SchemaRef,
    buffers: Vec<Vec<RecordBatch>>,
}

impl PartitionedMemorySink {
    pub fn new(
        expr: RowExpr,
        random_state: RandomState,
        schema: SchemaRef,
        num_partitions: usize,
    ) -> Self {
        Self {
            partitioner: Partitioner::new(expr, random_state, num_partitions),
            schema,
            buffers: vec![vec![]; num_partitions],
        }
    }
}

impl super::Sink for PartitionedMemorySink {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        let batches = self.partitioner.partition(&[batch])?;
        for (part, batch) in batches.into_iter().enumerate() {
            self.buffers[part].push(batch);
        }
        Ok(())
    }
}

pub struct MemorySource {
    schema: SchemaRef,
    unpartitioned: Vec<RecordBatch>,
    partitioned: Vec<Vec<RecordBatch>>,
}

impl super::LazyPartitionedSource for MemorySource {
    type PartitionedSource = Self;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        let memory = MemoryStream::try_new(
            std::mem::take(&mut self.unpartitioned),
            Arc::clone(&self.schema),
            None,
        )
        .expect("Failed to create memory stream");

        Ok(Box::pin(memory))
    }

    fn into_partitioned(self) -> Self {
        assert!(self.unpartitioned.is_empty());
        self
    }
}

impl PartitionedSource for MemorySource {
    async fn stream_partition(
        &mut self,
        index: super::PartitionIdx,
    ) -> SendableRecordBatchStream {
        Box::pin(
            MemoryStream::try_new(
                std::mem::take(&mut self.partitioned[index.0]),
                Arc::clone(&self.schema),
                None,
            )
            .unwrap(),
        )
    }
}

#[derive(Default)]
pub struct MemoryBuffer {}

impl LazyPartitionBuffer for MemoryBuffer {
    type Sink = MemorySink;
    type Source = MemorySource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        Ok(MemorySink {
            schema,
            buffers: vec![],
        })
    }

    fn make_source(&mut self, sink: Self::Sink) -> Result<Self::Source> {
        Ok(MemorySource {
            schema: sink.schema,
            unpartitioned: sink.buffers,
            partitioned: vec![],
        })
    }

    fn partition_count(&self) -> usize {
        0
    }
}

pub struct PartitionMemoryBuffer {
    expr: RowExpr,
    random_state: RandomState,
    num_partitions: usize,
}

impl PartitionMemoryBuffer {
    pub fn new(expr: RowExpr, random_state: RandomState, num_partitions: usize) -> Self {
        Self {
            expr,
            random_state,
            num_partitions,
        }
    }

    pub fn new_random(expr: RowExpr, num_partitions: usize) -> Self {
        Self::new(expr, Default::default(), num_partitions)
    }
}

impl LazyPartitionBuffer for PartitionMemoryBuffer {
    type Sink = PartitionedMemorySink;
    type Source = MemorySource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        Ok(PartitionedMemorySink::new(
            self.expr.clone(),
            self.random_state.clone(),
            schema,
            self.num_partitions,
        ))
    }

    fn make_source(&mut self, sink: Self::Sink) -> Result<Self::Source> {
        Ok(MemorySource {
            schema: sink.schema,
            unpartitioned: vec![],
            partitioned: sink.buffers,
        })
    }

    fn partition_count(&self) -> usize {
        self.num_partitions
    }
}
