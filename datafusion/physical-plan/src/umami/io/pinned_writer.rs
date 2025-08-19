use std::thread::JoinHandle;

use arrow::array::RecordBatch;
use tokio::sync::{mpsc, oneshot};

use crate::umami::io::{AsyncBatchWriter, BatchWriter, PartitionedBatchWriter};
use datafusion_common::{DataFusionError, Result};

pub fn make_pinned<Inner: PartitionedBatchWriter + Send + 'static>(
    inner: Inner,
) -> PinnedHandle<Inner> {
    let (sender, recv) = mpsc::channel(2);
    let actor = std::thread::spawn(move || Actor { recv, inner }.launch());
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
        self.sender.send(Message::Write {
            batch: batch,
            part: part,
            reply: tx,
        });

        rx.await
            .map_err(|recv_err| DataFusionError::Execution(recv_err.to_string()))?
    }

    async fn finish(&mut self) -> Result<Self::Intermediate> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Message::Finish { reply: tx });
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
                    reply.send(self.inner.write(&batch, part));
                }
                Message::Finish { reply } => {
                    reply.send(self.inner.finish());
                }
            }
        }
    }
}
