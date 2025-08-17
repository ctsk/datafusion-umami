use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

use crate::EmptyRecordBatchStream;

pub struct EmptySource {
    schema: SchemaRef,
}

impl EmptySource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl super::PartitionedSource for EmptySource {
    async fn stream_partition(
        &mut self,
        _index: super::PartitionIdx,
    ) -> SendableRecordBatchStream {
        Box::pin(EmptyRecordBatchStream::new(Arc::clone(&self.schema)))
    }
}

impl super::LazyPartitionedSource for EmptySource {
    type PartitionedSource = EmptySource;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
            &self.schema,
        ))))
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        self
    }
}
