use std::task::{ready, Poll};

use arrow::array::RecordBatch;
use datafusion_arrow_extra::compute;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

pub struct CoalesceStream {
    threshold: usize,
    buffered: Vec<RecordBatch>,
    buffered_num_rows: usize,
    input: SendableRecordBatchStream,
}

impl CoalesceStream {
    fn new(input: SendableRecordBatchStream, threshold: usize) -> Self {
        Self {
            input,
            threshold,
            buffered: vec![],
            buffered_num_rows: 0,
        }
    }

    pub fn stream(self) -> SendableRecordBatchStream {
        Box::pin(self) as _
    }

    fn concat_buffered(&mut self) -> Result<RecordBatch> {
        let concatted =
            compute::concat_batches_reduce_views(&self.input.schema(), &self.buffered)?;

        self.buffered.clear();
        self.buffered_num_rows = 0;

        Ok(concatted)
    }
}

impl RecordBatchStream for CoalesceStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }
}

impl Stream for CoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() >= self.threshold {
                        return Poll::Ready(Some(Ok(batch)));
                    } else {
                        self.buffered_num_rows += batch.num_rows();
                        self.buffered.push(batch);
                        if self.buffered_num_rows >= self.threshold {
                            return Poll::Ready(Some(self.concat_buffered()));
                        }
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    return Poll::Ready(Some(self.concat_buffered()));
                }
            }
        }
    }
}
