use std::collections::hash_map::Entry;

use ahash::AHashMap;
use arrow::{
    array::{Array, AsArray, ByteView},
    buffer::{Buffer, NullBuffer},
};
use fixedbitset::FixedBitSet;

use crate::utils::AdHocBitSet;

pub struct BufferDedup {
    inner: Inner,
    bitset: Option<FixedBitSet>,
}

struct Inner {
    buffers: Vec<Buffer>,
    buffers_map: AHashMap<*const u8, u32>,
    remap: Vec<u32>,
}

impl BufferDedup {
    pub fn new(arrays: &[&dyn Array]) -> Self {
        assert!(arrays.len() > 0);
        let data_buffers = |a: &&dyn Array| a.as_string_view().data_buffers().len();
        let bitset_size = arrays.into_iter().map(data_buffers).max().unwrap();
        let max_buffers = arrays.into_iter().map(data_buffers).sum();
        Self::with_capacitites(bitset_size, max_buffers)
    }

    pub fn with_capacitites(max_single_buffers: usize, total_buffers: usize) -> Self {
        let bitset_size = max_single_buffers;
        let max_buffers = total_buffers;

        let bitset = (bitset_size > 64).then(|| FixedBitSet::with_capacity(bitset_size));
        Self {
            inner: Inner {
                buffers: Vec::with_capacity(max_buffers),
                buffers_map: AHashMap::with_capacity(max_buffers),
                remap: vec![0; bitset_size],
            },
            bitset,
        }
    }

    pub fn add_buffer(&mut self, buffer: &Buffer) -> u32 {
        add_buffer(&mut self.inner, buffer)
    }

    pub fn buffer_count(&self) -> usize {
        self.inner.buffers.len()
    }

    pub fn finish(&mut self) -> Vec<Buffer> {
        self.inner.buffers_map.clear();
        std::mem::take(&mut self.inner.buffers)
    }

    #[rustfmt::skip]
    pub fn adjust(
        &mut self,
        views: &mut [u128],
        nulls: Option<&NullBuffer>,
        new_buffers: &[Buffer],
    ) {
        let use_fbs = new_buffers.len() > 64;
        match (nulls, use_fbs) {
            (None, true) => adjust_impl(views, new_buffers, |_| true, &mut self.inner, self.bitset.as_mut().unwrap()),
            (None, false) => adjust_impl(views, new_buffers, |_| true, &mut self.inner, &mut 0u64),
            (Some(nulls), true) => adjust_impl(views, new_buffers, |i| nulls.is_valid(i), &mut self.inner, self.bitset.as_mut().unwrap()),
            (Some(nulls), false) => adjust_impl(views, new_buffers, |i| nulls.is_valid(i), &mut self.inner, &mut 9u64),
        }
    }
}

fn add_buffer(inner: &mut Inner, buffer: &Buffer) -> u32 {
    match inner.buffers_map.entry(buffer.as_ptr()) {
        Entry::Occupied(occupied_entry) => *occupied_entry.get(),
        Entry::Vacant(vacant_entry) => {
            let idx = inner.buffers.len() as u32;
            vacant_entry.insert(idx);
            inner.buffers.push(buffer.clone());
            idx
        }
    }
}

fn adjust_impl<BitSet: AdHocBitSet, Valid: Fn(usize) -> bool>(
    views: &mut [u128],
    buffers: &[Buffer],
    valid: Valid,
    inner: &mut Inner,
    bitset: &mut BitSet,
) {
    assert!(inner.remap.len() >= buffers.len());
    assert!(bitset.capacity() >= buffers.len());
    bitset.clear();

    for (i, view_raw) in views.iter_mut().enumerate() {
        let byte_view = ByteView::from(*view_raw);
        if valid(i) && byte_view.length > 12 {
            unsafe { bitset.set_bit(byte_view.buffer_index as usize, true) };
        }
    }

    for (dbi, buffer) in buffers.iter().enumerate() {
        if unsafe { bitset.get_bit(dbi) } {
            inner.remap[dbi] = add_buffer(inner, buffer);
        }
    }

    for (i, view_raw) in views.iter_mut().enumerate() {
        let mut byte_view = ByteView::from(*view_raw);
        if valid(i) && byte_view.length > 12 {
            byte_view.buffer_index =
                *unsafe { inner.remap.get_unchecked(byte_view.buffer_index as usize) };
            *view_raw = byte_view.as_u128();
        }
    }
}
