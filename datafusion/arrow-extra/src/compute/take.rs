use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, GenericByteViewArray,
        PrimitiveArray,
    },
    compute::TakeOptions,
    datatypes::{BinaryViewType, ByteViewType, DataType, StringViewType},
    error::ArrowError,
};

use crate::utils::BufferDedup;

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

    let mut buffer_dedup = BufferDedup::new(&[array]);
    buffer_dedup.adjust(&mut new_views, new_nulls.as_ref(), array.data_buffers());
    let new_buffers = buffer_dedup.finish();

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
