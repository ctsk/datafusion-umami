use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

use super::{LazyPartitionedSource, PartitionedSource};
use crate::EmptyRecordBatchStream;

pub struct EmptySource {
    schema: SchemaRef,
    partition_count: usize,
}

impl EmptySource {
    pub fn new(schema: SchemaRef, partition_count: usize) -> Self {
        Self {
            schema,
            partition_count,
        }
    }
}

impl PartitionedSource for EmptySource {
    async fn stream_partition(
        &mut self,
        _index: super::PartitionIdx,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
            &self.schema,
        ))))
    }

    fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl LazyPartitionedSource for EmptySource {
    type PartitionedSource = EmptySource;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())))
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        self
    }
}
