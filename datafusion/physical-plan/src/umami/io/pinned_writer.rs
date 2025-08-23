use std::thread::JoinHandle;

use arrow::array::RecordBatch;
use tokio::sync::{mpsc, oneshot};

use crate::umami::io::{AsyncBatchWriter, PartitionedBatchWriter};
use datafusion_common::{DataFusionError, Result};

pub fn make_pinned<Inner, F>(inner: F) -> PinnedHandle<Inner>
where
    Inner: PartitionedBatchWriter + 'static,
    F: Send + 'static + FnOnce() -> Inner,
{
    let (sender, recv) = mpsc::channel(2);
    let actor = std::thread::spawn(move || {
        Actor {
            recv,
            inner: inner(),
        }
        .launch()
    });
    PinnedHandle { sender, actor }
}

pub struct PinnedHandle<Inner: PartitionedBatchWriter> {
    sender: mpsc::Sender<Message<Inner::Intermediate>>,
    actor: JoinHandle<()>,
}

impl<Inner: PartitionedBatchWriter> AsyncBatchWriter for PinnedHandle<Inner> {
    type Intermediate = Inner::Intermediate;

    async fn write(&mut self, batch: RecordBatch, part: usize) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Message::Write {
                batch: batch,
                part: part,
                reply: tx,
            })
            .await
            .expect("Lost contact to writer");

        rx.await
            .map_err(|recv_err| DataFusionError::Execution(recv_err.to_string()))?
    }

    async fn finish(&mut self) -> Result<Self::Intermediate> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Message::Finish { reply: tx })
            .await
            .expect("Lost onctact to writer :/");
        rx.await
            .map_err(|recv_err| DataFusionError::Execution(recv_err.to_string()))?
    }
}

enum Message<Intermediate> {
    Write {
        batch: RecordBatch,
        part: usize,
        reply: oneshot::Sender<Result<()>>,
    },
    Finish {
        reply: oneshot::Sender<Result<Intermediate>>,
    },
}

pub struct Actor<Inner: PartitionedBatchWriter> {
    recv: mpsc::Receiver<Message<Inner::Intermediate>>,
    inner: Inner,
}

impl<Inner: PartitionedBatchWriter> Actor<Inner> {
    fn launch(mut self) {
        while let Some(msg) = self.recv.blocking_recv() {
            match msg {
                Message::Write { batch, part, reply } => {
                    let _ = reply.send(self.inner.write(&batch, part));
                }
                Message::Finish { reply } => {
                    let _ = reply.send(self.inner.finish());
                }
            }
        }
    }
}
