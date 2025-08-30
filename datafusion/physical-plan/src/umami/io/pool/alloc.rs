//! Page mechanism without recycling

use std::ptr::NonNull;

use arrow::buffer::MutableBuffer;

#[derive(Default)]
pub struct AllocPool {}

pub struct AllocPage {
    inner: MutableBuffer,
}

impl super::Page for AllocPage {
    fn as_ptr(&mut self) -> NonNull<u8> {
        NonNull::new(self.inner.as_mut_ptr()).unwrap()
    }

    fn into_buffer(self) -> arrow::buffer::Buffer {
        self.inner.into()
    }
}

impl super::Pool for AllocPool {
    type Page = AllocPage;

    fn issue_page(&self, target: usize, _upper_bound: usize) -> Self::Page {
        let mut mbuf = MutableBuffer::new(target);
        unsafe { mbuf.set_len(target) };
        Self::Page { inner: mbuf }
    }
}
