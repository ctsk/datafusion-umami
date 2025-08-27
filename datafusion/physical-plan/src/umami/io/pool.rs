mod alloc;
mod local;
mod sendable;

use std::ptr::NonNull;

pub use alloc::AllocPool;
use arrow::buffer::Buffer;
pub use local::{LocalPage, LocalPool};
pub use sendable::SendablePool;

pub trait Pool {
    type Page: Page;

    fn issue_page(&self, size: usize) -> Self::Page;
}

pub trait Page {
    fn as_ptr(&mut self) -> NonNull<u8>;
    fn into_buffer(self) -> Buffer;
}
