use std::{cell::RefCell, mem::ManuallyDrop, rc::Rc};

pub struct LocalPool {
    inner: RefCell<Inner>,
    alloc_size: usize,
    limit: Option<usize>,
}

struct Inner {
    buffers: Vec<memmap2::MmapMut>,
    alloc_count: usize,
}

impl LocalPool {
    pub fn new(alloc_size: usize) -> Self {
        Self {
            inner: RefCell::new(Inner {
                buffers: Default::default(),
                alloc_count: 0,
            }),
            alloc_size,
            limit: None,
        }
    }

    pub fn new_with_limit(alloc_size: usize, limit: usize) -> Self {
        Self {
            inner: RefCell::new(Inner {
                buffers: Default::default(),
                alloc_count: 0,
            }),
            alloc_size,
            limit: Some(limit),
        }
    }

    pub fn issue_page(self: &Rc<Self>) -> LocalPage {
        let mut inner = self.inner.borrow_mut();
        let buffer = match inner.buffers.pop() {
            Some(buffer) => buffer,
            None => {
                inner.alloc_count += 1;
                if let Some(limit) = self.limit {
                    if inner.alloc_count > limit {
                        panic!("Exceeded limit of {} pages", limit);
                    }
                }
                self.allocate_new_page()
            }
        };

        LocalPage::new(Rc::clone(&self), buffer)
    }

    pub fn alloc_count(&self) -> usize {
        self.inner.borrow().alloc_count
    }

    pub fn can_issue_page(&self) -> bool {
        let inner = self.inner.borrow();
        !inner.buffers.is_empty() || inner.alloc_count < self.limit.unwrap_or(usize::MAX)
    }

    fn allocate_new_page(&self) -> memmap2::MmapMut {
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
            .inner
            .borrow_mut()
            .buffers
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
