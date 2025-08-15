use std::collections::hash_map::Entry;

use ahash::AHashMap;
use arrow::{
    array::{Array, AsArray, ByteView},
    buffer::{Buffer, NullBuffer},
};
use fixedbitset::FixedBitSet;

use crate::utils::AdHocBitSet;

pub struct BufferDedup {
    common: Common,
    bitset: Option<FixedBitSet>,
}

struct Common {
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

        let bitset = (max_buffers > 64).then(|| FixedBitSet::with_capacity(bitset_size));
        Self {
            common: Common {
                buffers: Vec::with_capacity(max_buffers),
                buffers_map: AHashMap::with_capacity(max_buffers),
                remap: vec![0; bitset_size],
            },
            bitset,
        }
    }

    pub fn buffer_count(&self) -> usize {
        self.common.buffers.len()
    }

    pub fn take(self) -> Vec<Buffer> {
        self.common.buffers
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
            (None, true) => adjust_impl(views, new_buffers, |_| true, &mut self.common, self.bitset.as_mut().unwrap()),
            (None, false) => adjust_impl(views, new_buffers, |_| true, &mut self.common, &mut 0u64),
            (Some(nulls), true) => adjust_impl(views, new_buffers, |i| nulls.is_valid(i), &mut self.common, self.bitset.as_mut().unwrap()),
            (Some(nulls), false) => adjust_impl(views, new_buffers, |i| nulls.is_valid(i), &mut self.common, &mut 9u64),
        }
    }
}

fn adjust_impl<BitSet: AdHocBitSet, Valid: Fn(usize) -> bool>(
    views: &mut [u128],
    buffers: &[Buffer],
    valid: Valid,
    common: &mut Common,
    bitset: &mut BitSet,
) {
    assert!(common.remap.len() >= buffers.len());
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
            common.remap[dbi] = match common.buffers_map.entry(buffer.as_ptr()) {
                Entry::Occupied(occupied_entry) => *occupied_entry.get(),
                Entry::Vacant(vacant_entry) => {
                    let idx = common.buffers.len() as u32;
                    vacant_entry.insert(idx);
                    common.buffers.push(buffer.clone());
                    idx
                }
            }
        }
    }

    for (i, view_raw) in views.iter_mut().enumerate() {
        let mut byte_view = ByteView::from(*view_raw);
        if valid(i) && byte_view.length > 12 {
            byte_view.buffer_index =
                *unsafe { common.remap.get_unchecked(byte_view.buffer_index as usize) };
            *view_raw = byte_view.as_u128();
        }
    }
}
