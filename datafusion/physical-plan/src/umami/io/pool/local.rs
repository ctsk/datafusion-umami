use std::{
    cell::RefCell,
    mem::ManuallyDrop,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

pub struct LocalPool {
    buffers: RefCell<Vec<memmap2::MmapMut>>,
    alloc_size: usize,
    alloc_count: AtomicU64,
}

impl LocalPool {
    pub fn new(alloc_size: usize) -> Self {
        Self {
            buffers: Default::default(),
            alloc_size,
            alloc_count: AtomicU64::new(0),
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
        self.alloc_count.fetch_add(1, Ordering::AcqRel);
        memmap2::MmapMut::map_anon(self.alloc_size)
            .expect("Failed to allocate spill page")
    }
}

pub struct LocalPage {
    pool: Rc<LocalPool>,
    vec: ManuallyDrop<Vec<u8>>,
    buffer: Option<memmap2::MmapMut>,
}

impl LocalPage {
    fn new(pool: Rc<LocalPool>, mut buffer: memmap2::MmapMut) -> Self {
        let vec = unsafe { Vec::from_raw_parts(buffer.as_mut_ptr(), 0, buffer.len()) };

        Self {
            pool,
            vec: ManuallyDrop::new(vec),
            buffer: Some(buffer),
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

    pub fn return_to_pool(&mut self) {
        self.pool
            .buffers
            .borrow_mut()
            .push(self.buffer.take().unwrap());
    }
}

impl Drop for LocalPage {
    fn drop(&mut self) {
        if self.buffer.is_some() {
            self.return_to_pool();
        }
    }
}
