use std::{
    cell::RefCell,
    mem::ManuallyDrop,
    rc::Rc,
    sync::{Arc, Mutex},
};

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
    pub fn issue_page(self: &Arc<Self>) -> SendablePage {
        let buffer = match self.inner.lock().unwrap().pop() {
            Some(buffer) => buffer,
            None => self.allocate_new_page(),
        };

        SendablePage {
            pool: Arc::clone(&self),
            buffer: Some(buffer),
        }
    }

    fn allocate_new_page(&self) -> memmap2::MmapMut {
        memmap2::MmapMut::map_anon(self.alloc_size)
            .expect("Failed to allocate spill page")
    }
}

pub struct SendablePage {
    pool: Arc<SendablePool>,
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

impl Drop for SendablePage {
    fn drop(&mut self) {
        self.return_to_pool();
    }
}

pub struct LocalPool {
    buffers: RefCell<Vec<memmap2::MmapMut>>,
    alloc_size: usize,
}

impl LocalPool {
    pub fn new(alloc_size: usize) -> Self {
        Self {
            buffers: Default::default(),
            alloc_size,
        }
    }
    pub fn issue_page(self: &Rc<Self>) -> LocalPage {
        let buffer = match self.buffers.borrow_mut().pop() {
            Some(buffer) => buffer,
            None => self.allocate_new_page(),
        };

        LocalPage::new(Rc::clone(&self), buffer)
    }

    fn allocate_new_page(&self) -> memmap2::MmapMut {
        memmap2::MmapMut::map_anon(self.alloc_size)
            .expect("Failed to allocate spill page")
    }
}

pub struct LocalPage {
    pool: Rc<LocalPool>,
    vec: ManuallyDrop<Vec<u8>>,
    buffer: memmap2::MmapMut,
}

impl LocalPage {
    fn new(pool: Rc<LocalPool>, mut buffer: memmap2::MmapMut) -> Self {
        let vec = unsafe { Vec::from_raw_parts(buffer.as_mut_ptr(), 0, buffer.len()) };

        Self {
            pool,
            vec: ManuallyDrop::new(vec),
            buffer,
        }
    }

    fn clear(&mut self) {
        self.vec.clear();
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn capacity(&self) -> usize {
        self.vec.capacity()
    }

    pub fn as_vec(&self) -> &Vec<u8> {
        self.vec.as_ref()
    }

    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        self.vec.as_mut()
    }

    pub fn return_to_pool(self) {
        self.pool.buffers.borrow_mut().push(self.buffer);
    }
}
