use std::future::Future;

use arrow::record_batch::RecordBatch;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;

mod adaptive;
mod async_spill;
mod empty;
mod memory;
mod spill;
mod uring_spill;

#[allow(unused_imports)]
pub use adaptive::AdaptiveBuffer;
#[allow(unused_imports)]
pub use adaptive::AdaptiveSinkConfig;
pub use async_spill::AsyncSpillBuffer;
#[allow(unused_imports)]
pub use memory::MemoryBuffer;
#[allow(unused_imports)]
pub use memory::PartitionMemoryBuffer;
pub use spill::SpillBuffer;
pub use uring_spill::IoUringSpillBuffer;

pub struct PartitionIdx(pub usize);

pub trait Sink {
    fn push(&mut self, batch: RecordBatch) -> impl Future<Output = Result<()>> + Send;
}

pub trait PartitionedSource {
    fn stream_partition(
        &mut self,
        index: PartitionIdx,
    ) -> impl Future<Output = SendableRecordBatchStream> + Send;
}

pub trait LazyPartitionedSource {
    type PartitionedSource: PartitionedSource + Send;

    fn unpartitioned(
        &mut self,
    ) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send;

    fn into_partitioned(self) -> Self::PartitionedSource;
}

pub trait LazyPartitionBuffer {
    type Sink: Sink + Send;
    type Source: LazyPartitionedSource + Send;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink>;
    fn make_source(
        &mut self,
        sink: Self::Sink,
    ) -> impl Future<Output = Result<Self::Source>> + Send;
    fn partition_count(&self) -> usize;
}
