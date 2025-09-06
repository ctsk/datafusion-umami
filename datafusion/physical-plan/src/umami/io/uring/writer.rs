use std::{
    collections::HashMap, os::fd::AsRawFd, os::unix::fs::OpenOptionsExt, path::PathBuf,
    rc::Rc,
};

use arrow::{
    array::RecordBatch,
    ipc::writer::{
        write_message, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
    },
};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_common::Result;
use io_uring::{opcode, types, IoUring};

use crate::umami::io::{
    aligned_ipc::{AlignedPartitionedIPC, BatchBlocks, Block, Loc},
    pool::{LocalPage, LocalPool},
    uring::WriteOpts,
};

enum Message {
    Sink(RecordBatch, usize),
    Finish,
}

pub struct Writer {
    path: PathBuf,
    sender: crossbeam::channel::Sender<Message>,
    worker: Option<std::thread::JoinHandle<Result<AlignedPartitionedIPC>>>,
}

impl Writer {
    pub fn new(path: PathBuf, schema: SchemaRef, parts: usize, opts: WriteOpts) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(opts.ring_depth * 2);
        let path_ = path.clone();
        let worker = std::thread::spawn(move || {
            PinnedWriter::new(schema, opts.ring_depth, Default::default(), path_, parts)
                .launch(rx, opts)
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

    async fn write(&mut self, batch: RecordBatch, part: usize) -> Result<()> {
        self.sender
            .send(Message::Sink(batch, part))
            .expect("Writer went missing");
        Ok(())
    }

    async fn finish(&mut self) -> Result<Self::Intermediate> {
        self.sender
            .send(Message::Finish)
            .expect("Writer went missing");

        self.worker.take().unwrap().join().unwrap()
    }
}

struct PinnedWriter {
    schema: SchemaRef,
    ring_depth: usize,
    write_options: IpcWriteOptions,
    path: PathBuf,
    partitions: usize,
    in_flight: usize,
    uring: IoUring,
    in_flight_counter: u64,
    in_flight_buffers: HashMap<u64, LocalPage>,
    pool: Rc<LocalPool>,
    data_gen: IpcDataGenerator,
    dict_tracker: DictionaryTracker,
}

impl PinnedWriter {
    pub fn new(
        schema: SchemaRef,
        ring_depth: usize,
        write_options: IpcWriteOptions,
        path: PathBuf,
        partitions: usize,
    ) -> Self {
        #[allow(deprecated)]
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(true, true);
        let data_gen = IpcDataGenerator::default();
        let _ = data_gen.schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut dict_tracker,
            &write_options,
        );

        let pool_limit: usize = partitions + ring_depth + 1;

        Self {
            schema,
            ring_depth,
            write_options,
            path,
            partitions,
            in_flight: 0,
            uring: IoUring::new(ring_depth as u32).expect("IoUring init failed."),
            in_flight_buffers: HashMap::new(),
            in_flight_counter: 0,
            pool: Rc::new(LocalPool::new_with_limit(
                super::BATCH_UPPER_BOUND,
                pool_limit,
            )),
            data_gen,
            dict_tracker,
        }
    }

    fn reap(&mut self) -> Result<()> {
        self.uring
            .submit_and_wait(1)
            .expect("Waiting for uring completion failed.");
        self.soft_reap();
        assert!(self.in_flight < self.ring_depth);
        Ok(())
    }

    fn soft_reap(&mut self) {
        let mut cq = unsafe { self.uring.completion_shared() };
        cq.sync();
        for cqe in cq {
            let result = cqe.result();
            self.in_flight -= 1;
            if result < 0 {
                log::error!("I/O write failed.")
            }
            let id = cqe.user_data();
            self.in_flight_buffers.remove(&id);
        }
    }

    fn encode_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(Vec<EncodedData>, EncodedData), ArrowError> {
        self.data_gen
            .encoded_batch(batch, &mut self.dict_tracker, &self.write_options)
    }
}

impl PinnedWriter {
    fn launch(
        &mut self,
        rx: crossbeam::channel::Receiver<Message>,
        wopts: WriteOpts,
    ) -> Result<AlignedPartitionedIPC> {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true);
        if wopts.direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }
        let file = opts.open(&self.path)?;
        let mut state_per_p: Vec<_> = (0..self.partitions)
            .map(|_| PartWriteState::new(self.write_options.clone()))
            .collect();

        // We allow partitions + depth pages to be in the system;
        // We allow `depth` submissions to be in the queue right now.

        let mut next_write_offset = 0;

        while let Ok(msg) = rx.recv() {
            match msg {
                Message::Sink(batch, part) => {
                    let (dicts, batch) = self.encode_batch(&batch)?;
                    self.soft_reap();
                    if !state_per_p[part].has_page() {
                        if !self.pool.can_issue_page() {
                            self.reap()?; // Hard reap required to make a page available
                        }
                        let page = self.pool.issue_page();
                        state_per_p[part].assign_page(page);
                    }

                    state_per_p[part].push(dicts, batch)?;

                    if state_per_p[part].buffered_size() > wopts.write_lower_bound {
                        self.soft_reap();
                        if self.in_flight >= self.ring_depth {
                            self.reap()?; // Hard reap required to make room for more submissions
                        }

                        let page =
                            state_per_p[part].take_page_for_write(next_write_offset);
                        let op = opcode::Write::new(
                            types::Fd(file.as_raw_fd()),
                            page.as_vec().as_ptr(),
                            page.len() as u32,
                        )
                        .offset(next_write_offset as u64)
                        .build()
                        .user_data(self.in_flight_counter);

                        next_write_offset += page.len();
                        let mut submission = self.uring.submission();
                        submission.sync();
                        unsafe {
                            submission.push(&op).expect("submission queue is full");
                        }
                        drop(submission);
                        self.uring.submit()?;
                        self.in_flight_buffers.insert(self.in_flight_counter, page);
                        self.in_flight_counter += 1;
                        self.in_flight += 1;
                    }
                }
                Message::Finish => {
                    let mut pbbs = Vec::new();
                    for mut state in state_per_p {
                        if state.buffered_size() > 0 {
                            self.soft_reap();
                            if self.in_flight >= self.ring_depth {
                                self.reap()?; // Hard reap required to make room for more submissions
                            }

                            let page = state.take_page_for_write(next_write_offset);
                            let op = opcode::Write::new(
                                types::Fd(file.as_raw_fd()),
                                page.as_vec().as_ptr(),
                                page.len() as u32,
                            )
                            .offset(next_write_offset as u64)
                            .build()
                            .user_data(self.in_flight_counter);

                            next_write_offset += page.len();
                            let mut submission = self.uring.submission();
                            submission.sync();
                            unsafe {
                                submission.push(&op).expect("submission queue is full");
                            }
                            drop(submission);
                            self.uring.submit()?;
                            self.in_flight_buffers.insert(self.in_flight_counter, page);
                            self.in_flight_counter += 1;
                            self.in_flight += 1;
                        }
                        pbbs.push((state.offsets, state.written_batches))
                    }

                    self.uring.submit_and_wait(self.in_flight)?;
                    self.in_flight = 0;

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
            blocks: vec![],
        });
    }
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
    page: Option<LocalPage>,
    unwritten_batches: Vec<BatchBlocks>,
    offsets: Vec<Loc>,
    written_batches: Vec<BatchBlocks>,
    write_options: IpcWriteOptions,
}

impl PartWriteState {
    fn new(write_options: IpcWriteOptions) -> Self {
        Self {
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

    fn has_page(&self) -> bool {
        self.page.is_some()
    }

    fn assign_page(&mut self, page: LocalPage) {
        self.page = Some(page);
    }

    fn push(&mut self, dicts: Vec<EncodedData>, batch: EncodedData) -> Result<()> {
        assert!(self.has_page());
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
