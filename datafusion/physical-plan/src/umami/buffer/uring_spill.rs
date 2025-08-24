use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{
    disk_manager::RefCountedTempFile, runtime_env::RuntimeEnv, SendableRecordBatchStream,
};

use crate::umami::{
    buffer::{empty, LazyPartitionBuffer, LazyPartitionedSource},
    io::{self, AsyncBatchWriter},
};

pub struct IoUringSink {
    file: RefCountedTempFile,
    schema: SchemaRef,
    writer: io::uring::Writer,
}

pub struct IoUringSpillBuffer {
    runtime: Arc<RuntimeEnv>,
}

impl IoUringSpillBuffer {
    pub const NAME: &str = "UMAMI_URING_SPILL";

    pub fn new(runtime: Arc<RuntimeEnv>) -> Self {
        Self { runtime }
    }
}

impl super::Sink for IoUringSink {
    async fn push(&mut self, batch: arrow::array::RecordBatch) -> Result<()> {
        let batch = crate::common::compact(1.0, batch);
        self.writer.write(batch, 0).await
    }
}

pub struct IoUringSource {
    file: RefCountedTempFile,
    schema: SchemaRef,
    reader: io::uring::Reader,
}

impl LazyPartitionedSource for IoUringSource {
    type PartitionedSource = empty::EmptySource;

    async fn unpartitioned(&mut self) -> Result<SendableRecordBatchStream> {
        Ok(self.reader.launch(0))
    }

    fn into_partitioned(self) -> Self::PartitionedSource {
        empty::EmptySource::new(self.schema)
    }
}

impl LazyPartitionBuffer for IoUringSpillBuffer {
    type Sink = IoUringSink;
    type Source = IoUringSource;

    fn make_sink(&mut self, schema: SchemaRef) -> Result<Self::Sink> {
        let file = self.runtime.disk_manager.create_tmp_file(Self::NAME)?;
        let writer =
            io::uring::Writer::new(file.path().to_owned(), Arc::clone(&schema), 1);
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
        };
        Ok(source)
    }

    fn partition_count(&self) -> usize {
        0
    }
}
