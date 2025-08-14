use std::future::Future;

use arrow::record_batch::RecordBatch;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

mod empty;
mod memory;

pub use memory::MemoryBuffer;

pub trait Sink {
    fn push(&mut self, batch: RecordBatch) -> impl Future<Output = Result<()>> + Send;
}

pub struct PartitionIdx(pub u32);

pub trait PartitionedSource {
    async fn stream_partition(
        &mut self,
        index: PartitionIdx,
    ) -> SendableRecordBatchStream;
}

pub trait LazyPartitionedSource {
    type PartitionedSource: PartitionedSource;

    fn stream_unpartitioned(
        &mut self,
    ) -> impl Future<Output = SendableRecordBatchStream> + Send;

    async fn into_partitioned(self) -> Self::PartitionedSource;
}

pub trait LazyPartitionBuffer {
    type Sink: Sink + Send;
    type Source: LazyPartitionedSource + Send;

    fn make_sink(schema: SchemaRef) -> Self::Sink;
    fn make_source(sink: Self::Sink) -> Self::Source;
}
