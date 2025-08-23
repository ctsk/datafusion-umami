use std::{
    cell::RefCell, ops::Range, os::fd::AsRawFd, path::PathBuf, ptr::NonNull, rc::Rc,
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{reader::FileDecoder, MetadataVersion},
};
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::SendableRecordBatchStream;
use io_uring::{opcode, types};
use io_uring_async::IoUringAsync;
use tokio::sync::{mpsc, Semaphore};

use crate::{
    stream::RecordBatchReceiverStreamBuilder,
    umami::io::{
        aligned_ipc::{AlignedPartitionedIPC, BatchBlocks, Loc},
        pool::{SendablePage, SendablePool},
    },
};

pub struct Reader {
    schema: SchemaRef,
    path: PathBuf,
    blocks_per_p: Vec<Option<(Vec<Loc>, Vec<BatchBlocks>)>>,
}

impl Reader {
    pub fn new(data: AlignedPartitionedIPC) -> Self {
        Self {
            schema: data.schema,
            path: data.path,
            blocks_per_p: data.blocks.into_iter().map(Some).collect(),
        }
    }

    pub fn launch(&mut self, part: usize) -> SendableRecordBatchStream {
        let (locs, bbss) = self.blocks_per_p[part]
            .take()
            .expect("Whoa! Partition was read multiple times");

        let schema = Arc::clone(&self.schema);
        let path = self.path.clone();
        let builder = RecordBatchReceiverStreamBuilder::new(
            Arc::clone(&self.schema),
            super::IO_URING_DEPTH,
        );
        let sender = builder.tx();

        std::thread::spawn(move || {
            PinnedReader {
                schema,
                path,
                offsets: locs,
                batches: Rc::new(bbss),
                depth: super::IO_URING_DEPTH as u32,
            }
            .launch(sender)
        });

        builder.build()
    }
}

struct PinnedReader {
    schema: SchemaRef,
    offsets: Vec<Loc>,
    batches: Rc<Vec<BatchBlocks>>,
    path: PathBuf,
    depth: u32,
}

use super::DIRECT_IO_ALIGNMENT;

impl PinnedReader {
    fn make_decoder(schema: SchemaRef) -> FileDecoder {
        unsafe {
            FileDecoder::new(schema, MetadataVersion(MetadataVersion::ENUM_MAX))
                .with_skip_validation(true)
        }
    }

    async fn decode_and_send(
        decoder: Rc<RefCell<FileDecoder>>,
        batches: Rc<Vec<BatchBlocks>>,
        block_range: Range<usize>,
        mut page: SendablePage,
        loc: Loc,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        assert!(!page.as_mut_ptr().is_null());
        let nn = unsafe { NonNull::new_unchecked(page.as_mut_ptr()) };
        let buffer =
            unsafe { Buffer::from_custom_allocation(nn, loc.length, Arc::new(page)) };
        for batch_blocks in &batches[block_range] {
            for dict in batch_blocks.dicts.iter() {
                let buf = buffer.slice_with_length(
                    dict.offset - loc.file_offset,
                    dict.meta_length + dict.data_length,
                );
                decoder.borrow_mut().read_dictionary(&dict.to_ipc(), &buf)?;
            }

            let mut block = batch_blocks.batch.clone();
            block.offset -= loc.file_offset;
            let buf = buffer.slice_with_length(block.offset, block.length());
            let result = Ok(decoder
                .borrow()
                .read_record_batch(&block.to_ipc(), &buf)
                .transpose()
                .unwrap()?);
            sender.send(result).await.unwrap()
        }
        Ok(())
    }

    pub fn launch(self, sender: mpsc::Sender<Result<RecordBatch>>) -> Result<()> {
        let uring = IoUringAsync::new(self.depth).unwrap();
        let uring = Rc::new(uring);
        let file = std::fs::OpenOptions::new()
            .create(false)
            .read(true)
            .open(&self.path)?;

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

        let decoder = Rc::new(RefCell::new(Self::make_decoder(Arc::clone(&self.schema))));

        let pool = Arc::new(SendablePool::new(super::BATCH_UPPER_BOUND));
        let limiter = Arc::new(Semaphore::new(self.depth as usize * 2));

        runtime.block_on(async move {
            tokio::task::LocalSet::new()
                .run_until(async {
                    // Spawn a task that waits for the io_uring to become readable and handles completion
                    // queue entries accordingly.
                    tokio::task::spawn_local(IoUringAsync::listen(uring.clone()));
                    let mut last_offset = 0;
                    for loc in self.offsets {
                        let permit = Arc::clone(&limiter).acquire_owned().await.unwrap();
                        let mut page = pool.issue_page();
                        assert!(page.as_mut_ptr() as usize % DIRECT_IO_ALIGNMENT == 0);
                        assert!(loc.length as usize % DIRECT_IO_ALIGNMENT == 0);
                        assert!(loc.file_offset as usize % DIRECT_IO_ALIGNMENT == 0);
                        let read_e = opcode::Read::new(
                            types::Fd(file.as_raw_fd()),
                            page.as_mut_ptr(),
                            loc.length as u32,
                        )
                        .offset(loc.file_offset as u64)
                        .build();

                        let uring = Rc::clone(&uring);
                        let batches = Rc::clone(&self.batches);
                        let range = last_offset..loc.batch_blocks_offset;
                        let sender = sender.clone();
                        let decoder = Rc::clone(&decoder);
                        last_offset = loc.batch_blocks_offset;
                        tokio::task::spawn_local(async move {
                            let cqe = uring.push(read_e).await;
                            if cqe.result() < 0 {
                                panic!("Read returned error {}", cqe.result());
                            }
                            Self::decode_and_send(
                                decoder, batches, range, page, loc, sender,
                            )
                            .await
                            .unwrap();
                            drop(permit);
                        });
                    }

                    let _permits = limiter.acquire_many(self.depth * 2).await.unwrap();
                    limiter.close();

                    drop(sender);
                })
                .await
        });

        Ok(())
    }
}
