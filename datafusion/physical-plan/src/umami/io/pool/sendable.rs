use std::{
    ops::Deref,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use arrow::buffer::Buffer;

pub struct SendablePool {
    inner: Mutex<Vec<memmap2::MmapMut>>,
    alloc_size: usize,
}

impl SendablePool {
    pub fn new(alloc_size: usize) -> Self {
        Self {
            inner: Default::default(),
            alloc_size,
        }
    }

    fn allocate_new_page(&self) -> memmap2::MmapMut {
        memmap2::MmapMut::map_anon(self.alloc_size)
            .expect("Failed to allocate spill page")
    }
}

pub struct SendablePage {
    pool: Arc<SendablePool>,
    // ptr: NonNull<u8>,
    buffer: Option<memmap2::MmapMut>,
}

impl SendablePage {
    fn return_to_pool(&mut self) {
        let buffer = self.buffer.take().unwrap();
        self.pool.inner.lock().unwrap().push(buffer);
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buffer.as_mut().unwrap().as_mut_ptr()
    }
}

impl Deref for SendablePage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().unwrap().deref()
    }
}

impl<T> AsRef<T> for SendablePage
where
    T: ?Sized,
    <SendablePage as Deref>::Target: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.deref().as_ref()
    }
}

impl Drop for SendablePage {
    fn drop(&mut self) {
        self.return_to_pool();
    }
}

impl super::Page for SendablePage {
    fn as_ptr(&mut self) -> NonNull<u8> {
        let buffer = self.buffer.as_mut().unwrap();
        unsafe { NonNull::new_unchecked(buffer.as_mut_ptr()) }
    }

    fn into_buffer(mut self) -> Buffer {
        unsafe {
            let buffer = self.buffer.as_mut().unwrap();

            Buffer::from_custom_allocation(
                NonNull::new_unchecked(buffer.as_mut_ptr()),
                buffer.len(),
                Arc::new(self),
            )
        }
    }
}

impl super::Pool for Arc<SendablePool> {
    type Page = SendablePage;

    fn issue_page(&self, size: usize) -> SendablePage {
        let buffer = match self.inner.lock().unwrap().pop() {
            Some(buffer) => buffer,
            None => self.allocate_new_page(),
        };

        assert!(size <= buffer.len());

        SendablePage {
            pool: Arc::clone(&self),
            buffer: Some(buffer),
        }
    }
}
