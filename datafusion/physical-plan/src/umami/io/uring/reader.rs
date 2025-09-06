use std::{
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
    ptr::NonNull,
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{reader::FileDecoder, Block, MetadataVersion},
};
use arrow_schema::SchemaRef;
use datafusion_common::{HashMap, Result};
use datafusion_execution::RecordBatchStream;
use futures::Stream;
use io_uring::{opcode::Read, types, IoUring};
use tokio::sync::mpsc;

pub struct ReaderActor {
    file: std::fs::File,
    data: SinglePartOOMData,
    uring: IoUring,
    depth: usize,
    in_flight_count: usize,
    next_id: usize,
    in_flight: HashMap<usize, pool::Page>,
    schema: SchemaRef,
    decoder: FileDecoder,
    cached: Option<RecordBatch>,
}

struct AlignedBuffer {
    cap: usize,
    ptr: NonNull<u8>,
}

unsafe impl Sync for AlignedBuffer {}
unsafe impl Send for AlignedBuffer {}

impl AlignedBuffer {
    unsafe fn allocate(capacity: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align_unchecked(
            capacity,
            DIRECT_IO_ALIGNMENT as usize,
        );

        let ptr = std::alloc::alloc(layout);

        Self {
            cap: capacity,
            ptr: NonNull::new(ptr).unwrap(),
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            let layout = std::alloc::Layout::from_size_align_unchecked(
                self.cap,
                DIRECT_IO_ALIGNMENT as usize,
            );

            std::alloc::dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl ReaderActor {
    pub fn new(path: &Path, schema: SchemaRef, data: SinglePartOOMData) -> Self {
        Self::new_with_options(path, schema, data, Default::default())
    }

    pub fn new_with_options(
        path: &Path,
        schema: SchemaRef,
        data: SinglePartOOMData,
        options: Options,
    ) -> Self {
        let decoder = unsafe {
            FileDecoder::new(
                Arc::clone(&schema),
                MetadataVersion(MetadataVersion::ENUM_MAX),
            )
            .with_skip_validation(true)
        };

        Self {
            schema,
            file: std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)
                .unwrap(),
            data,
            depth: options.depth as usize,
            uring: IoUring::new(options.depth).unwrap(),
            in_flight_count: 0,
            in_flight: Default::default(),
            next_id: 0,
            decoder,
            cached: None,
        }
    }

    pub fn slice_block(block: &Block, buffer: &Buffer) -> Buffer {
        assert!(
            block.bodyLength() as usize + block.metaDataLength() as usize <= buffer.len()
        );

        let offset = block.offset() as usize;
        //let length = block.bodyLength() as usize + block.metaDataLength() as usize;

        //buffer.slice_with_length(offset, length)
        buffer.slice(offset)
    }

    fn decode(&mut self, idx: usize, mut buffer: pool::Page) -> Result<RecordBatch> {
        let blocks = &self.data.batches[idx];
        let ptr = buffer.as_mut_ptr();
        let len = buffer.capacity();
        let owner = Arc::new(buffer);

        let buffer = unsafe { Buffer::from_custom_allocation(ptr, len, owner) };

        for dict_block in blocks.dicts.iter() {
            let buf = Self::slice_block(&dict_block.block, &buffer);
            self.decoder.read_dictionary(&dict_block.block, &buf)?;
        }

        let buf = Self::slice_block(&blocks.batch.block, &buffer);
        match self.decoder.read_record_batch(&blocks.batch.block, &buf) {
            Ok(rb) => Ok(rb.unwrap()),
            Err(e) => {
                eprint!("{:#?}", &self.data.batches[..10]);
                println!("idx: {idx:?} -- {blocks:?}");
                Err(e.into())
            }
        }
    }

    pub fn read_next(&mut self) -> Result<Option<RecordBatch>> {
        self.read_next_async()
    }

    pub fn read_next_sync(&mut self) -> Result<Option<RecordBatch>> {
        if self.next_id >= self.data.batches.len() {
            return Ok(None);
        }

        let id = self.next_id;
        self.next_id += 1;

        let blocks = &self.data.batches[id];
        let mut buffer = pool::Pool::get(pool::local_pool());

        assert_eq!(blocks.length % DIRECT_IO_ALIGNMENT, 0);
        assert_eq!(blocks.offset % DIRECT_IO_ALIGNMENT, 0);
        assert_eq!(
            buffer.as_mut_ptr().as_ptr() as usize % DIRECT_IO_ALIGNMENT,
            0
        );

        // read via io_uring

        let read_event = Read::new(
            types::Fd(self.file.as_raw_fd()),
            buffer.as_mut_ptr().as_ptr(),
            blocks.length as u32,
        )
        .offset(blocks.offset as u64)
        .build()
        .user_data(id as u64);

        unsafe { self.uring.submission().push(&read_event).unwrap() };
        self.uring.submit_and_wait(1).unwrap();
        let cqe = self.uring.completion().next().unwrap();
        assert!(cqe.result() >= 0);

        if let Some(batch) = self.cached.as_ref() {
            return Ok(Some(batch.clone()));
        } else {
            let batch = self.decode(id, buffer)?;
            self.cached = Some(batch.clone());
            return Ok(Some(batch));
        }
    }

    pub fn read_next_async(&mut self) -> Result<Option<RecordBatch>> {
        if self.next_id >= self.data.batches.len() {
            return Ok(None);
        }

        // 4 Phases:
        //    - 1. Check for a completion
        //    - 2. Fill up for capacity
        //    - 3. If no completion in 1, wait, else, return batch
        //    - 4. Fill up again, do not wait
        //    - 5. Return batch

        // 1
        let maybe_completion = self.uring.completion().next();
        let maybe_batch = if let Some(completion) = maybe_completion {
            let result = completion.result();
            if result < 0 {
                log::debug!("URING read error: {result}");
                return crate::internal_err!("URING read failed");
            }
            let id = completion.user_data() as usize;
            let buffer = self.in_flight.remove(&id).unwrap();
            self.in_flight_count -= 1;
            Some(self.decode(id, buffer)?)
        } else {
            None
        };

        // 2
        for _ in self.in_flight_count..self.depth {
            if self.next_id >= self.data.batches.len() {
                break;
            }

            let id = self.next_id;
            self.next_id += 1;

            let blocks = &self.data.batches[id];
            let mut buffer = pool::Pool::get(pool::local_pool());
            assert!(buffer.capacity() >= blocks.length);

            let read_event = Read::new(
                types::Fd(self.file.as_raw_fd()),
                buffer.as_mut_ptr().as_ptr(),
                blocks.length as u32,
            )
            .offset(blocks.offset as u64)
            .build()
            .user_data(id as u64);

            unsafe { self.uring.submission().push(&read_event).unwrap() };

            self.in_flight.insert(id, buffer);
            self.in_flight_count += 1;
        }

        // 3
        if let Some(batch) = maybe_batch {
            return Ok(Some(batch));
        }

        self.uring.submit_and_wait(1).unwrap();
        let completion = self.uring.completion().next().unwrap();
        let result = completion.result();
        if result < 0 {
            log::debug!("URING read error: {result}");
            return crate::internal_err!("URING read failed");
        }
        let id = completion.user_data() as usize;
        let buffer = self.in_flight.remove(&id).unwrap();
        self.in_flight_count -= 1;
        Ok(Some(self.decode(id, buffer)?))
    }
}

pub struct Reader {
    schema: SchemaRef,
    inner: mpsc::Receiver<Result<RecordBatch>>,
}

impl Reader {
    pub fn new(path: &Path, schema: SchemaRef, data: SinglePartOOMData) -> Self {
        let mut inner = ReaderActor::new(path, Arc::clone(&schema), data);
        let (tx, rx) = mpsc::channel(1);
        std::thread::spawn(move || loop {
            let batch = inner.read_next();

            match batch {
                Ok(Some(batch)) => tx.blocking_send(Ok(batch)),
                Ok(None) => {
                    drop(tx);
                    return;
                }
                Err(e) => {
                    tx.blocking_send(Err(e));
                    return;
                }
            };
        });
        Self { schema, inner: rx }
    }
}

impl RecordBatchStream for Reader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for Reader {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}
