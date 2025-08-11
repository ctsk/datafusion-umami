use std::sync::Arc;

use datafusion_common::arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, ByteView, GenericByteViewArray,
        NullBufferBuilder, PrimitiveArray,
    },
    buffer::{NullBuffer, ScalarBuffer},
    datatypes::{ArrowNativeType, ByteViewType, DataType},
    downcast_primitive_array,
};

#[allow(unused)]
pub fn scatter(
    indices: &[Vec<u64>],
    hist: &[usize],
    array: &[&dyn Array],
) -> Vec<ArrayRef> {
    let arr = array[0];
    downcast_primitive_array!(
        arr => {
            scatter_primitive::<_, false>(indices, hist, &to(arr, array))
        }
        DataType::Utf8View => {
            scatter_view(indices, hist, &to(arr.as_string_view(), array))
        }
        DataType::BinaryView => {
            scatter_view(indices, hist, &to(arr.as_binary_view(), array))
        }
        _ => {
            todo!()
        }
    )
}

fn to<'a, 'b, T: Array + 'static>(init: &T, arr: &'a [&'b dyn Array]) -> Vec<&'b T> {
    assert!(arr.iter().all(|a| a.data_type() == init.data_type()));
    arr.into_iter()
        .map(|a| a.as_any().downcast_ref::<T>().unwrap())
        .collect()
}

#[inline(never)]
fn scatter_primitive<T: ArrowPrimitiveType, const WRITE_COMBINE: bool>(
    indices: &[Vec<u64>],
    hist: &[usize],
    arrays: &[&PrimitiveArray<T>],
) -> Vec<ArrayRef> {
    assert_eq!(indices.len(), arrays.len());
    let mut out: Vec<_> = hist.iter().map(|i| Vec::with_capacity(*i)).collect();

    for (&array, indices) in arrays.into_iter().zip(indices.into_iter()) {
        assert_eq!(indices.len(), array.len());
        scatter_native(indices, &array.values(), &mut out);
    }

    let nulls = if needs_scatter_nulls(arrays) {
        scatter_nulls_arr(indices, hist, arrays)
    } else {
        vec![None; hist.len()]
    };

    out.into_iter()
        .zip(nulls)
        .map(|(values, nulls)| {
            let b = ScalarBuffer::from(Vec::from(values));
            Arc::new(
                PrimitiveArray::<T>::new(b, nulls)
                    .with_data_type(arrays[0].data_type().clone()),
            ) as _
        })
        .collect()
}

#[inline(never)]
fn scatter_native<T: ArrowNativeType>(indices: &[u64], inp: &[T], out: &mut [Vec<T>]) {
    for (i, dst) in indices.iter().enumerate() {
        unsafe {
            let out_vec = out.get_unchecked_mut(*dst as usize);
            let idx = out_vec.len();
            out_vec.set_len(idx + 1);
            *out_vec.get_unchecked_mut(idx) = *inp.get_unchecked(i);
        }
    }
}

fn scatter_native_adjust_buffer(
    indices: &[u64],
    inp: &[u128],
    out: &mut [Vec<u128>],
    add: u32,
) {
    for (i, dst) in indices.iter().enumerate() {
        out[*dst as usize].push({
            let v = unsafe { inp.get_unchecked(i) };
            let mut v = ByteView::from(*v);
            if v.length > 12 {
                v.buffer_index += add;
            }
            v.as_u128()
        });
    }
}

#[inline(never)]
fn scatter_view<T: ByteViewType>(
    indices: &[Vec<u64>],
    hist: &[usize],
    arrays: &[&GenericByteViewArray<T>],
) -> Vec<ArrayRef> {
    assert_eq!(indices.len(), arrays.len());
    let mut out: Vec<_> = hist.iter().map(|i| Vec::with_capacity(*i)).collect();

    let data_buffers: Vec<_> = arrays
        .into_iter()
        .flat_map(|a| a.data_buffers().iter().cloned())
        .collect();

    let mut adjust = 0;
    for (array, indices) in arrays.into_iter().zip(indices.into_iter()) {
        assert_eq!(indices.len(), array.len());
        scatter_native_adjust_buffer(indices, &array.views(), &mut out, adjust);
        adjust += array.data_buffers().len() as u32;
    }

    let nulls = if needs_scatter_nulls(arrays) {
        scatter_nulls_arr(indices, hist, arrays)
    } else {
        vec![None; hist.len()]
    };

    out.into_iter()
        .zip(nulls)
        .map(|(values, nulls)| {
            let b = ScalarBuffer::from(values);
            Arc::new(unsafe {
                GenericByteViewArray::<T>::new_unchecked(b, data_buffers.clone(), nulls)
            }) as _
        })
        .collect()
}

fn needs_scatter_nulls<T: Array>(arrays: &[&T]) -> bool {
    arrays.iter().any(|a| a.null_count() > 0)
}

fn scatter_nulls_arr<T: Array>(
    indices: &[Vec<u64>],
    hist: &[usize],
    arrays: &[&T],
) -> Vec<Option<NullBuffer>> {
    let mut out: Vec<_> = hist.iter().map(|i| NullBufferBuilder::new(*i)).collect();

    for (&array, indices) in arrays.into_iter().zip(indices.into_iter()) {
        scatter_nulls(indices, array.nulls(), &mut out);
    }

    out.into_iter()
        .map(|mut builder| builder.finish())
        .collect()
}

#[inline(never)]
fn scatter_nulls(
    indices: &[u64],
    nulls: Option<&NullBuffer>,
    out: &mut [NullBufferBuilder],
) {
    // todo: perf and optimize this method
    match nulls.filter(|n| n.null_count() > 0) {
        Some(nulls) => {
            for (i, dst) in indices.iter().enumerate() {
                unsafe {
                    let builder = out.get_unchecked_mut(*dst as usize);
                    builder.append(nulls.inner().value_unchecked(i));
                }
            }
        }
        None => {
            for dst in indices.iter() {
                unsafe {
                    let builder = out.get_unchecked_mut(*dst as usize);
                    builder.append_non_null();
                }
            }
        }
    }
}
