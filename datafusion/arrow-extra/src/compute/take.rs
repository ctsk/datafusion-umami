use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, ByteView, GenericByteViewArray,
        PrimitiveArray,
    },
    compute::TakeOptions,
    datatypes::{BinaryViewType, ByteViewType, DataType, StringViewType},
    error::ArrowError,
};

/// take_reduce_views delegates to [`arrow::compute::take`] except for
/// StringView/BinaryView columns. In those cases, it only creates references
/// to buffers that are actually referenced in the newly-created array.
pub fn take_reduce_views<T: ArrowPrimitiveType>(
    values: &dyn Array,
    indices: &PrimitiveArray<T>,
    opts: Option<TakeOptions>,
) -> Result<ArrayRef, ArrowError> {
    match values.data_type() {
        DataType::Utf8View => {
            take_reduce_views_impl::<StringViewType, _>(values, indices, opts)
        }
        DataType::BinaryView => {
            take_reduce_views_impl::<BinaryViewType, _>(values, indices, opts)
        }
        _ => arrow::compute::take(values, indices, opts),
    }
}

fn take_reduce_views_impl<V: ByteViewType, T: ArrowPrimitiveType>(
    array: &dyn Array,
    indices: &PrimitiveArray<T>,
    opts: Option<TakeOptions>,
) -> Result<ArrayRef, ArrowError> {
    assert!(
        opts.is_none(),
        "TakeOptions are not yet supported for this take_reduce_views"
    );

    let array = array.as_byte_view::<V>();

    let mut new_views = internal::take_native(array.views(), indices);
    let new_nulls = internal::take_nulls(array.nulls(), indices);

    let new_buffers = if array.data_buffers().len() > 1 {
        let estimate = if array.len() > 0 {
            array.data_buffers().len() * indices.len() / array.len()
        } else {
            0
        };

        let mut new_buffers = Vec::with_capacity(estimate);
        let mut buffers = HashMap::new();
        for (idx, view) in new_views.iter_mut().enumerate() {
            let mut byte_view = ByteView::from(*view);
            if byte_view.length > 12
                && new_nulls
                    .as_ref()
                    .map(|null| null.is_valid(idx))
                    .unwrap_or(true)
            {
                let buffer_index = byte_view.buffer_index as usize;
                let buffer = unsafe { array.data_buffers().get_unchecked(buffer_index) };
                // todo: take whole buffer and length into account (and fix-up offset)....
                let ptr = buffer.as_ptr();
                match buffers.entry(ptr) {
                    Entry::Occupied(occupied_entry) => {
                        byte_view.buffer_index = *occupied_entry.get() as u32;
                    }
                    Entry::Vacant(vacant_entry) => {
                        byte_view.buffer_index = new_buffers.len() as u32;
                        vacant_entry.insert(new_buffers.len());
                        new_buffers.push(buffer.clone());
                    }
                }
                *view = byte_view.as_u128();
            }
        }

        new_buffers
    } else {
        array.data_buffers().to_vec()
    };

    // Safety:  array.views was valid, and take_native copies only valid values, and verifies bounds
    Ok(Arc::new(unsafe {
        GenericByteViewArray::<V>::new_unchecked(new_views.into(), new_buffers, new_nulls)
    }))
}

mod internal {
    use arrow::{
        array::{Array, ArrowPrimitiveType, PrimitiveArray},
        buffer::{BooleanBuffer, MutableBuffer, NullBuffer},
        datatypes::ArrowNativeType,
        util::bit_util,
    };

    #[inline(never)]
    pub(super) fn take_native<T: ArrowNativeType, I: ArrowPrimitiveType>(
        values: &[T],
        indices: &PrimitiveArray<I>,
    ) -> Vec<T> {
        match indices.nulls().filter(|n| n.null_count() > 0) {
            Some(n) => indices
                .values()
                .iter()
                .enumerate()
                .map(|(idx, index)| match values.get(index.as_usize()) {
                    Some(v) => *v,
                    None => match n.is_null(idx) {
                        true => T::default(),
                        false => panic!("Out-of-bounds index {index:?}"),
                    },
                })
                .collect(),
            None => indices
                .values()
                .iter()
                .map(|index| values[index.as_usize()])
                .collect(),
        }
    }

    #[inline(never)]
    pub(super) fn take_nulls<I: ArrowPrimitiveType>(
        values: Option<&NullBuffer>,
        indices: &PrimitiveArray<I>,
    ) -> Option<NullBuffer> {
        match values.filter(|n| n.null_count() > 0) {
            Some(n) => {
                let buffer = take_bits(n.inner(), indices);
                Some(NullBuffer::new(buffer)).filter(|n| n.null_count() > 0)
            }
            None => indices.nulls().cloned(),
        }
    }

    #[inline(never)]
    fn take_bits<I: ArrowPrimitiveType>(
        values: &BooleanBuffer,
        indices: &PrimitiveArray<I>,
    ) -> BooleanBuffer {
        let len = indices.len();

        match indices.nulls().filter(|n| n.null_count() > 0) {
            Some(nulls) => {
                let mut output_buffer = MutableBuffer::new_null(len);
                let output_slice = output_buffer.as_slice_mut();
                nulls.valid_indices().for_each(|idx| {
                    if values.value(indices.value(idx).as_usize()) {
                        bit_util::set_bit(output_slice, idx);
                    }
                });
                BooleanBuffer::new(output_buffer.into(), 0, len)
            }
            None => {
                BooleanBuffer::collect_bool(len, |idx: usize| {
                    // SAFETY: idx<indices.len()
                    values.value(unsafe { indices.value_unchecked(idx).as_usize() })
                })
            }
        }
    }
}
