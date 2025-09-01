use std::{
    sync::Arc,
    task::{ready, Poll},
};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

pub struct ChainedStream {
    schema: SchemaRef,
    inner: Vec<SendableRecordBatchStream>,
}

impl ChainedStream {
    pub fn new(schema: SchemaRef, inner: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, inner }
    }

    pub fn stream(self) -> SendableRecordBatchStream {
        Box::pin(self) as _
    }
}

impl RecordBatchStream for ChainedStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for ChainedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.inner.last_mut() {
                Some(stream) => match ready!(stream.poll_next_unpin(cx)) {
                    Some(batch) => return Poll::Ready(Some(batch)),
                    None => {
                        self.inner.pop();
                    }
                },
                None => return Poll::Ready(None),
            }
        }
    }
}
