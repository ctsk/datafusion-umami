use std::{
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::PathBuf,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use arrow::{
    array::RecordBatch,
    ipc::writer::{
        write_message, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
    },
};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use io_uring::{opcode, types};
use io_uring_async::IoUringAsync;
use send_wrapper::SendWrapper;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::umami::io::{
    aligned_ipc::{AlignedPartitionedIPC, BatchBlocks, Block, Loc},
    pool::{LocalPage, LocalPool},
};

pub struct Writer {
    path: PathBuf,
    sender: mpsc::Sender<Message>,
    worker: Option<std::thread::JoinHandle<Result<AlignedPartitionedIPC>>>,
}

impl Writer {
    pub fn new(path: PathBuf, schema: SchemaRef, parts: usize) -> Self {
        let (tx, rx) = mpsc::channel(1);
        let path_ = path.clone();
        let worker = std::thread::spawn(move || {
            PinnedWriter::new(
                schema,
                super::IO_URING_DEPTH,
                Default::default(),
                path_,
                parts,
            )
            .launch(rx)
        });

        Self {
            path,
            sender: tx,
            worker: Some(worker),
        }
    }
}

impl super::super::AsyncBatchWriter for Writer {
    type Intermediate = AlignedPartitionedIPC;

    async fn write(&mut self, batch: RecordBatch, partition: usize) -> Result<()> {
        self.sender.send(Message::Sink(batch, partition)).await;
        Ok(())
    }

    async fn finish(&mut self) -> Result<Self::Intermediate> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Message::Finish(tx)).await;
        rx.await;
        self.worker.take().unwrap().join().unwrap()
    }
}

struct PinnedWriter {
    schema: SchemaRef,
    dict_tracker: DictionaryTracker,
    data_gen: IpcDataGenerator,
    write_options: IpcWriteOptions,
    depth: usize,
    path: PathBuf,
    partitions: usize,
}

enum Message {
    Sink(RecordBatch, usize),
    Finish(oneshot::Sender<()>),
}

use super::DIRECT_IO_ALIGNMENT;

fn direct_io_pad(value: usize) -> usize {
    ((value + DIRECT_IO_ALIGNMENT - 1) / DIRECT_IO_ALIGNMENT) * DIRECT_IO_ALIGNMENT
}

fn direct_io_pad_vec<T: Copy>(vec: &mut Vec<T>, value: T) {
    let new_len = ((vec.len() + DIRECT_IO_ALIGNMENT - 1) / DIRECT_IO_ALIGNMENT)
        * DIRECT_IO_ALIGNMENT;
    assert!(vec.capacity() >= new_len);
    vec.resize(new_len, value);
}

struct PartWriteState {
    pool: Rc<LocalPool>,
    page: Option<LocalPage>,
    unwritten_batches: Vec<BatchBlocks>,
    offsets: Vec<Loc>,
    written_batches: Vec<BatchBlocks>,
    write_options: IpcWriteOptions,
}

impl PartWriteState {
    fn new(pool: Rc<LocalPool>, write_options: IpcWriteOptions) -> Self {
        Self {
            pool,
            page: None,
            unwritten_batches: Vec::new(),
            offsets: vec![],
            written_batches: vec![],
            write_options,
        }
    }
}

impl PartWriteState {
    fn push_data(
        page: &mut LocalPage,
        encoded: EncodedData,
        write_options: &IpcWriteOptions,
    ) -> Result<Block> {
        let offset = page.len();
        let ptr_before = page.as_vec().as_ptr() as usize;
        let dst: &mut Vec<u8> = page.as_mut_vec();
        let (meta, data) = write_message(dst, encoded, &write_options)?;
        let ptr_after = page.as_vec().as_ptr() as usize;

        assert!(
            ptr_before == ptr_after,
            "Caution: Page moved during spilling!"
        );

        Ok(Block {
            meta_length: meta,
            data_length: data,
            offset,
        })
    }

    fn push(&mut self, dicts: Vec<EncodedData>, batch: EncodedData) -> Result<()> {
        if self.page.is_none() {
            self.page = Some(self.pool.issue_page());
        }
        let page_ref = self.page.as_mut().unwrap();
        let mut dict_blocks = Vec::new();
        for dict in dicts {
            dict_blocks.push(Self::push_data(page_ref, dict, &self.write_options)?);
        }
        let batch = Self::push_data(page_ref, batch, &self.write_options)?;
        direct_io_pad_vec(page_ref.as_mut_vec(), 0);
        let bb = BatchBlocks {
            dicts: dict_blocks,
            batch,
        };
        self.unwritten_batches.push(bb);
        Ok(())
    }

    fn buffered_size(&self) -> usize {
        match self.page {
            Some(ref page) => page.len(),
            None => 0,
        }
    }

    fn take_page_for_write(&mut self, offset: usize) -> LocalPage {
        assert!(self.page.is_some());
        let page = self.page.take().unwrap();
        let num_batches = self.unwritten_batches.len();
        let batch_blocks_offset = self.written_batches.len() + num_batches;
        self.offsets.push(Loc {
            batch_blocks_offset,
            file_offset: offset,
            length: page.len(),
        });
        self.written_batches
            .extend(self.unwritten_batches.drain(..).map(|mut bb| {
                for dict in bb.dicts.iter_mut() {
                    dict.offset += offset;
                }
                bb.batch.offset += offset;
                bb
            }));
        page
    }
}

impl PinnedWriter {
    fn new(
        schema: SchemaRef,
        depth: usize,
        write_options: IpcWriteOptions,
        path: PathBuf,
        parts: usize,
    ) -> Self {
        let mut dict_tracker = DictionaryTracker::new(true);
        let data_gen = IpcDataGenerator::default();
        let _ = data_gen.schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut dict_tracker,
            &write_options,
        );

        Self {
            schema,
            dict_tracker,
            data_gen,
            write_options,
            depth,
            path,
            partitions: parts,
        }
    }

    fn encode_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        self.data_gen
            .encoded_batch(batch, &mut self.dict_tracker, &self.write_options)
    }

    fn launch(
        mut self,
        mut receiver: mpsc::Receiver<Message>,
    ) -> Result<AlignedPartitionedIPC> {
        let uring = IoUringAsync::new(self.depth as u32).unwrap();
        let uring = Rc::new(uring);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&self.path)?;

        // Create a new current_thread runtime that submits all outstanding submission queue
        // entries as soon as the executor goes idle.
        let uring_clone = SendWrapper::new(uring.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .on_thread_park(move || {
                uring_clone.submit().unwrap();
            })
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async move {
            tokio::task::LocalSet::new()
                .run_until(async {
                    // Spawn a task that waits for the io_uring to become readable and handles completion
                    // queue entries accordingly.
                    tokio::task::spawn_local(IoUringAsync::listen(uring.clone()));
                    let limiter = Arc::new(Semaphore::new(self.depth));
                    let mut next_write_offset = 0;
                    let mut pool = Rc::new(LocalPool::new(super::BATCH_UPPER_BOUND));
                    let mut state_per_p: Vec<_> = (0..self.partitions)
                        .map(|_| {
                            PartWriteState::new(
                                Rc::clone(&pool),
                                self.write_options.clone(),
                            )
                        })
                        .collect();
                    let abort = Rc::new(AtomicBool::new(false));
                    while let Some(msg) = receiver.recv().await {
                        if abort.load(Ordering::Relaxed) {
                            return Err(DataFusionError::Execution(String::from(
                                "An error occured while writing",
                            )));
                        }

                        match msg {
                            Message::Sink(record_batch, part) => {
                                let (dicts, batch) = self.encode_batch(&record_batch)?;
                                state_per_p[part].push(dicts, batch);

                                if state_per_p[part].buffered_size()
                                    >= super::WRITE_LOWER_BOUND
                                {
                                    let permit = limiter.clone().acquire_owned();
                                    let page = state_per_p[part]
                                        .take_page_for_write(next_write_offset);
                                    let uring = uring.clone();
                                    let op = opcode::Write::new(
                                        types::Fd(file.as_raw_fd()),
                                        page.as_vec().as_ptr(),
                                        page.len() as u32,
                                    )
                                    .offset(next_write_offset as u64)
                                    .build();
                                    next_write_offset += page.len();
                                    let abort = abort.clone();
                                    tokio::task::spawn_local(async move {
                                        let cqe = uring.push(op).await;
                                        if cqe.result() < 0 {
                                            abort.store(true, Ordering::Release);
                                        }
                                        page.return_to_pool();
                                        drop(permit)
                                    });
                                }
                            }
                            Message::Finish(sender) => {
                                let permits = limiter.acquire_many(self.depth as u32);

                                let mut pbbs = Vec::new();
                                for mut state in state_per_p {
                                    if state.buffered_size() > 0 {
                                        let page =
                                            state.take_page_for_write(next_write_offset);
                                        let uring = uring.clone();
                                        let ptr_as_usize =
                                            page.as_vec().as_ptr() as usize;
                                        assert!(ptr_as_usize % DIRECT_IO_ALIGNMENT == 0);
                                        assert!(page.len() % DIRECT_IO_ALIGNMENT == 0);
                                        assert!(
                                            next_write_offset % DIRECT_IO_ALIGNMENT == 0
                                        );
                                        let op = opcode::Write::new(
                                            types::Fd(file.as_raw_fd()),
                                            page.as_vec().as_ptr(),
                                            page.len() as u32,
                                        )
                                        .offset(next_write_offset as u64)
                                        .build();
                                        next_write_offset += page.len();
                                        let cqe = uring.push(op).await;
                                        if cqe.result() < 0 {
                                            abort.store(true, Ordering::Release);
                                        }
                                        page.return_to_pool();
                                    }

                                    pbbs.push((state.offsets, state.written_batches))
                                }

                                sender.send(());

                                return Ok(AlignedPartitionedIPC {
                                    schema: self.schema.clone(),
                                    path: self.path.clone(),
                                    blocks: pbbs,
                                });
                            }
                        }
                    }

                    unreachable!()
                })
                .await
        })
    }
}
