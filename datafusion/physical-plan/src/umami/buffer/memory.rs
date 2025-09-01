use std::sync::Arc;

use ahash::RandomState;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

use crate::{
    memory::MemoryStream,
    repartition::Partitioner,
    umami::buffer::{LazyPartitionBuffer, LazyPartitionedSource, PartitionedSource},
    utils::RowExpr,
};

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

impl super::PartitionedSink for PartitionedMemorySink {
    async fn push_to_part(&mut self, batch: RecordBatch, part: usize) -> Result<()> {
        self.buffers[part].push(batch);
        Ok(())
    }
}

impl super::Sink for PartitionedMemorySink {
    async fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if self.buffers.len() > 1 {
            let batches = self.partitioner.partition(&[batch])?;
            for (part, batch) in batches.into_iter().enumerate() {
                self.buffers[part].push(batch);
            }
        } else {
            self.buffers[0].push(batch);
        }
        Ok(())
    }

    async fn force_partition(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct MemorySource {
    schema: SchemaRef,
    unpartitioned: Vec<RecordBatch>,
    partitioned: Vec<Vec<RecordBatch>>,
}

impl MemorySource {
    pub fn new(
        schema: SchemaRef,
        unpartitioned: Vec<RecordBatch>,
        partitioned: Vec<Vec<RecordBatch>>,
    ) -> Self {
        Self {
            schema,
            unpartitioned,
            partitioned,
        }
    }
}

impl LazyPartitionedSource for MemorySource {
    type PartitionedSource = Self;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        let memory = MemoryStream::try_new(
            std::mem::take(&mut self.unpartitioned),
            Arc::clone(&self.schema),
            None,
        )?;

        Ok(Box::pin(memory))
    }

    fn into_partitioned(self) -> Self {
        assert!(self.unpartitioned.is_empty());
        self
    }

    async fn all_in_mem(&mut self) -> Result<SendableRecordBatchStream> {
        let mut batches = std::mem::take(&mut self.unpartitioned);
        for mut part in self.partitioned.iter_mut() {
            batches.append(&mut part);
        }

        let memory = MemoryStream::try_new(batches, Arc::clone(&self.schema), None)?;
        Ok(Box::pin(memory))
    }
}

impl PartitionedSource for MemorySource {
    async fn stream_partition(
        &mut self,
        index: super::PartitionIdx,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(
            MemoryStream::try_new(
                std::mem::take(&mut self.partitioned[index.0]),
                Arc::clone(&self.schema),
                None,
            )
            .unwrap(),
        ))
    }

    fn partition_count(&self) -> usize {
        0
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

    fn make_sink(&mut self, schema: SchemaRef, _key: RowExpr) -> Result<Self::Sink> {
        Ok(PartitionedMemorySink::new(
            self.expr.clone(),
            self.random_state.clone(),
            schema,
            self.num_partitions,
        ))
    }

    async fn make_source(&mut self, sink: Self::Sink) -> Result<Self::Source> {
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
