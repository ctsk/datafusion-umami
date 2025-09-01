use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::{config::ExperimentalOptions, Result};
use datafusion_execution::{
    disk_manager::RefCountedTempFile, runtime_env::RuntimeEnv, SendableRecordBatchStream,
};

use crate::umami::{
    buffer::{PartitionBuffer, PartitionedSource},
    io::{self, AsyncBatchWriter},
};

pub struct IoUringSink {
    file: RefCountedTempFile,
    schema: SchemaRef,
    writer: io::uring::Writer,
}

pub struct IoUringSpillBuffer {
    runtime: Arc<RuntimeEnv>,
    recycle: bool,
    direct_io_reader: bool,
    direct_io_writer: bool,
    partition_count: usize,
}

impl IoUringSpillBuffer {
    pub const NAME: &str = "UMAMI_URING_SPILL";

    pub fn new(runtime: Arc<RuntimeEnv>, x: &ExperimentalOptions) -> Self {
        Self {
            runtime,
            recycle: x.recycle,
            direct_io_reader: x.direct_io_reader,
            direct_io_writer: x.direct_io_writer,
            partition_count: x.part_count,
        }
    }
}

impl super::PartitionedSink for IoUringSink {
    async fn push_to_part(
        &mut self,
        batch: arrow::array::RecordBatch,
        part: usize,
    ) -> Result<()> {
        let batch = crate::common::compact(1.0, batch);
        self.writer.write(batch, part).await
    }
}

pub struct IoUringSource {
    file: RefCountedTempFile,
    schema: SchemaRef,
    reader: io::uring::Reader,
    recycle: bool,
    direct_io: bool,
    partition_count: usize,
}

impl PartitionedSource for IoUringSource {
    async fn stream_partition(
        &mut self,
        index: super::PartitionIdx,
    ) -> Result<SendableRecordBatchStream> {
        Ok(self.reader.launch(index.0, self.recycle, self.direct_io))
    }

    fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl PartitionBuffer for IoUringSpillBuffer {
    type Sink = IoUringSink;
    type Source = IoUringSource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        let file = self.runtime.disk_manager.create_tmp_file(Self::NAME)?;
        let writer = io::uring::Writer::new(
            file.path().to_owned(),
            Arc::clone(&schema),
            self.partition_count(),
            self.direct_io_writer,
        );
        Ok(Self::Sink {
            file,
            schema,
            writer,
        })
    }

    async fn make_source(&mut self, mut sink: Self::Sink) -> Result<Self::Source> {
        let data = sink.writer.finish().await?;
        let reader = io::uring::Reader::new(data);
        let source = IoUringSource {
            file: sink.file,
            schema: sink.schema,
            reader,
            recycle: self.recycle,
            direct_io: self.direct_io_reader,
            partition_count: self.partition_count,
        };
        Ok(source)
    }

    fn partition_count(&self) -> usize {
        self.partition_count
    }
}
