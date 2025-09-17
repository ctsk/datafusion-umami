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
        write_message_by_ref, DictionaryTracker, EncodedData, EncoderState,
        IpcDataGenerator, IpcWriteOptions,
    },
};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use io_uring::{opcode, types};
use io_uring_async::IoUringAsync;
use once_cell::unsync::OnceCell;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::umami::io::{
    aligned_ipc::{AlignedPartitionedIPC, BatchBlocks, Block, Loc},
    pool::{LocalPage, LocalPool},
    uring::WriteOpts,
};

pub struct Writer {
    init: Option<Init>,
    once_inner: OnceCell<Inner>,
}

struct Init {
    opts: WriteOpts,
    schema: SchemaRef,
    path: PathBuf,
    partitions: usize,
}

struct Inner {
    sender: mpsc::Sender<Message>,
    worker: Option<std::thread::JoinHandle<Result<AlignedPartitionedIPC>>>,
}

impl Writer {
    fn inner(&mut self) -> &mut Inner {
        self.once_inner.get_or_init(|| {
            let init = self.init.take().unwrap();
            let (tx, rx) = mpsc::channel((init.partitions * 2).max(16));

            let worker = std::thread::spawn(move || {
                PinnedWriter::new(
                    init.schema,
                    init.opts.ring_depth,
                    Default::default(),
                    init.path,
                    init.partitions,
                )
                .launch(rx, init.opts)
            });

            Inner {
                sender: tx,
                worker: Some(worker),
            }
        });

        self.once_inner.get_mut().unwrap()
    }

    pub fn new(path: PathBuf, schema: SchemaRef, parts: usize, opts: WriteOpts) -> Self {
        Self {
            init: Some(Init {
                schema,
                opts,
                path,
                partitions: parts,
            }),
            once_inner: OnceCell::new(),
        }
    }
}

impl super::super::AsyncBatchWriter for Writer {
    type Intermediate = AlignedPartitionedIPC;

    async fn write(&mut self, batch: RecordBatch, partition: usize) -> Result<()> {
        self.inner()
            .sender
            .send(Message::Sink(batch, partition))
            .await
            .unwrap();
        Ok(())
    }

    async fn finish(&mut self) -> Result<Self::Intermediate> {
        match OnceCell::get_mut(&mut self.once_inner) {
            Some(inner) => {
                let (tx, rx) = oneshot::channel();
                inner.sender.send(Message::Finish(tx)).await.unwrap();
                let _ = rx.await;
                inner.worker.take().unwrap().join().unwrap()
            }
            None => {
                let init = self.init.take().unwrap();
                Ok(AlignedPartitionedIPC {
                    schema: init.schema,
                    path: init.path,
                    blocks: vec![(vec![], vec![]); init.partitions],
                })
            }
        }
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

#[allow(dead_code)]
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
        ipc_message: &[u8],
        arrow_data: &[u8],
        write_options: &IpcWriteOptions,
    ) -> Result<Block> {
        let offset = page.len();
        let ptr_before = page.as_vec().as_ptr() as usize;
        let dst: &mut Vec<u8> = page.as_mut_vec();
        let (meta, data) =
            write_message_by_ref(dst, ipc_message, arrow_data, &write_options)?;
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

    fn push(&mut self, dicts: &[EncodedData], batch: &EncoderState) -> Result<()> {
        if self.page.is_none() {
            self.page = Some(self.pool.issue_page());
        }
        let page_ref = self.page.as_mut().unwrap();
        let mut dict_blocks = Vec::new();
        for dict in dicts {
            dict_blocks.push(Self::push_data(
                page_ref,
                &dict.ipc_message,
                &dict.arrow_data,
                &self.write_options,
            )?);
        }
        let batch = Self::push_data(
            page_ref,
            batch.ipc_message().unwrap(),
            batch.arrow_data().unwrap(),
            &self.write_options,
        )?;
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
        #[allow(deprecated)]
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(true, true);
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
        state: &mut EncoderState,
    ) -> Result<Vec<EncodedData>, ArrowError> {
        self.data_gen.encoded_batch_to(
            batch,
            &mut self.dict_tracker,
            &self.write_options,
            state,
        )
    }

    fn launch(
        mut self,
        mut receiver: mpsc::Receiver<Message>,
        wopts: WriteOpts,
    ) -> Result<AlignedPartitionedIPC> {
        let uring = IoUringAsync::new(self.depth as u32).unwrap();
        let uring = Rc::new(uring);
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true).write(true);
        if wopts.direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }
        let file = opts.open(&self.path)?;

        // Create a new current_thread runtime that submits all outstanding submission queue
        // entries as soon as the executor goes idle.
        let uring_clone = super::RefSendWrapper::new(uring.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .on_thread_park(move || {
                uring_clone.as_ref().submit().unwrap();
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
                    let limiter = Arc::new(Semaphore::new(self.depth * 2));
                    let mut next_write_offset = 0;
                    let pool = Rc::new(LocalPool::new(super::BATCH_UPPER_BOUND));
                    let mut state_per_p: Vec<_> = (0..self.partitions)
                        .map(|_| {
                            PartWriteState::new(
                                Rc::clone(&pool),
                                self.write_options.clone(),
                            )
                        })
                        .collect();
                    let abort = Rc::new(AtomicBool::new(false));
                    let mut encoder_state = EncoderState::new();
                    while let Some(msg) = receiver.recv().await {
                        if abort.load(Ordering::Relaxed) {
                            return Err(DataFusionError::Execution(String::from(
                                "An error occured while writing",
                            )));
                        }

                        match msg {
                            Message::Sink(record_batch, part) => {
                                let dicts =
                                    self.encode_batch(&record_batch, &mut encoder_state)?;
                                state_per_p[part].push(&dicts, &encoder_state)?;

                                if state_per_p[part].buffered_size()
                                    >= wopts.write_lower_bound
                                {
                                    let permit =
                                        limiter.clone().acquire_owned().await.unwrap();
                                    let mut page = state_per_p[part]
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
                                let _permits = limiter
                                    .acquire_many(self.depth as u32 * 2)
                                    .await
                                    .unwrap();

                                limiter.close();

                                let mut pbbs = Vec::new();
                                for mut state in state_per_p {
                                    if state.buffered_size() > 0 {
                                        let mut page =
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

                                sender.send(()).unwrap();

                                return Ok(AlignedPartitionedIPC {
                                    schema: self.schema.clone(),
                                    path: self.path.clone(),
                                    blocks: pbbs,
                                });
                            }
                        }
                    }

                    return Ok(AlignedPartitionedIPC {
                        schema: self.schema.clone(),
                        path: self.path.clone(),
                        blocks: Vec::new(),
                    });
                    // unreachable!()
                })
                .await
        })
    }
}
