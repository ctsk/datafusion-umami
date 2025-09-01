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
pub use adaptive::StaticHybridSinkConfig;
pub use async_spill::AsyncSpillBuffer;
#[allow(unused_imports)]
pub use memory::PartitionMemoryBuffer;
pub use spill::SpillBuffer;
pub use uring_spill::IoUringSpillBuffer;

use crate::umami::filter;
use crate::umami::report::BufferReport;
use crate::utils::ChainedStream;
use crate::utils::RowExpr;

#[derive(Copy, Clone)]
pub struct PartitionIdx(pub usize);

pub trait PartitionedSink {
    fn push_to_part(
        &mut self,
        batch: RecordBatch,
        part: usize,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub trait Sink: PartitionedSink {
    fn push(&mut self, batch: RecordBatch) -> impl Future<Output = Result<()>> + Send;
    fn force_partition(&mut self) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PartState {
    Owned,
    Delegated,
}

pub trait PartitionedSource: Send {
    fn stream_partition(
        &mut self,
        index: PartitionIdx,
    ) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send;

    fn stream_partitions(
        &mut self,
        indices: &[PartitionIdx],
    ) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send {
        assert!(!indices.is_empty());
        async move {
            let mut streams = Vec::with_capacity(indices.len());
            for index in indices {
                match self.stream_partition(*index).await {
                    Ok(stream) => streams.push(stream),
                    Err(e) => return Err(e),
                }
            }

            Ok(Box::pin(ChainedStream::new(streams[0].schema(), streams)) as _)
        }
    }

    fn partition_sequence(&self) -> impl Iterator<Item = usize> {
        0..self.partition_count()
    }

    fn partition_count(&self) -> usize;

    fn schema(&self) -> SchemaRef;
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

    fn make_sink(&mut self, schema: SchemaRef, key: RowExpr) -> Result<Self::Sink>;

    fn make_filtered_stream(
        &mut self,
        _sink: &Self::Sink,
        _stream: SendableRecordBatchStream,
        _key: RowExpr,
    ) -> Result<(SendableRecordBatchStream, filter::Airlock<Self::Sink>)> {
        unimplemented!("make_filter_sink is not yet implemented.")
    }

    fn probe_sink(&self, _sink: &Self::Sink) -> BufferReport {
        unimplemented!("probe is not yet implemented.")
    }

    fn make_source(
        &mut self,
        sink: Self::Sink,
    ) -> impl Future<Output = Result<Self::Source>> + Send;

    fn partition_count(&self) -> usize;
}

pub trait PartitionBuffer {
    type Sink: PartitionedSink + Send;
    type Source: PartitionedSource + Send;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink>;
    fn make_source(
        &mut self,
        sink: Self::Sink,
    ) -> impl Future<Output = Result<Self::Source>> + Send;
    fn partition_count(&self) -> usize;
}

pub struct SinglePartAdapter<S: PartitionedSink> {
    pub inner: S,
}

impl<S: PartitionedSink + Send> PartitionedSink for SinglePartAdapter<S> {
    async fn push_to_part(&mut self, batch: RecordBatch, part: usize) -> Result<()> {
        self.inner.push_to_part(batch, part).await
    }
}

impl<S: PartitionedSink + Send> Sink for SinglePartAdapter<S> {
    fn push(&mut self, batch: RecordBatch) -> impl Future<Output = Result<()>> + Send {
        self.inner.push_to_part(batch, 0)
    }

    async fn force_partition(&mut self) -> Result<()> {
        Ok(())
    }
}

struct LazySourceAdapter<S>(S);

impl<S: PartitionedSource + Send> LazyPartitionedSource for LazySourceAdapter<S> {
    type PartitionedSource = S;

    fn unpartitioned(
        &mut self,
    ) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send {
        let schema = self.0.schema();
        async { empty::EmptySource::new(schema, 0).unpartitioned().await }
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        self.0
    }
}
