use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, BooleanBufferBuilder,
        ByteView, GenericByteArray, GenericByteViewArray, NullBufferBuilder,
        PrimitiveArray,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{ArrowNativeType, ByteArrayType, ByteViewType, DataType},
    downcast_primitive_array,
};

use crate::utils::BufferDedup;

#[rustfmt::skip]
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
        DataType::Boolean => scatter_boolean(indices, hist, &to(arr.as_boolean(), array)),
        DataType::Utf8View => scatter_view(indices, hist, &to(arr.as_string_view(), array)),
        DataType::BinaryView => scatter_view(indices, hist, &to(arr.as_binary_view(), array)),
        DataType::Utf8 => scatter_bytes(indices, hist, &to(arr.as_string::<i32>(), array)),
        DataType::LargeUtf8 => scatter_bytes(indices, hist, &to(arr.as_string::<i64>(), array)),
        DataType::Binary => scatter_bytes(indices, hist, &to(arr.as_binary::<i32>(), array)),
        DataType::LargeBinary => scatter_bytes(indices, hist, &to(arr.as_binary::<i64>(), array)),
        _ => {
            todo!("Scattering {:?} is not yet supported", arr.data_type())
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
    let mut out: Vec<_> = hist.iter().map(|l| Vec::with_capacity(*l)).collect();

    let nulls = if needs_scatter_nulls(arrays) {
        scatter_nulls_arr(indices, hist, arrays)
    } else {
        vec![None; hist.len()]
    };

    let mut adjust = 0;
    for (array, indices) in arrays.into_iter().zip(indices.into_iter()) {
        assert_eq!(indices.len(), array.len());
        scatter_native_adjust_buffer(indices, array.views(), &mut out, adjust);
        adjust += array.data_buffers().len() as u32;
    }

    let all_data_buffers = arrays
        .iter()
        .flat_map(|a| a.data_buffers().iter())
        .cloned()
        .collect::<Vec<_>>();

    let mut dedup =
        BufferDedup::with_capacitites(all_data_buffers.len(), all_data_buffers.len());

    out.into_iter()
        .zip(nulls)
        .map(|(mut views, nulls)| {
            dedup.adjust(&mut views, nulls.as_ref(), &all_data_buffers);
            Arc::new(unsafe {
                GenericByteViewArray::<T>::new_unchecked(
                    views.into(),
                    dedup.finish(),
                    nulls,
                )
            }) as _
        })
        .collect()
}

#[inline(never)]
fn compute_data_size<T: ByteArrayType>(
    indices: &[u64],
    array: &GenericByteArray<T>,
    data_hist: &mut [usize],
) {
    for (i, dst) in indices.iter().enumerate() {
        unsafe {
            let raw: &[u8] = array.value_unchecked(i).as_ref();
            *data_hist.get_unchecked_mut(*dst as usize) += raw.len();
        }
    }
}

#[inline(never)]
fn scatter_bytes<T: ByteArrayType>(
    indices: &[Vec<u64>],
    hist: &[usize],
    arrays: &[&GenericByteArray<T>],
) -> Vec<ArrayRef> {
    // 1. Compute size of the data section of each output array
    let mut data_hist = vec![0; hist.len()];
    for (indices, array) in indices.iter().zip(arrays.iter()) {
        compute_data_size(indices, array, &mut data_hist);
    }

    let mut out = Vec::from_iter(
        hist.iter()
            .zip(data_hist.iter())
            .map(|(ol, dl)| (Vec::with_capacity(*ol + 1), Vec::with_capacity(*dl))),
    );

    for (ol, _) in out.iter_mut() {
        ol.push(unsafe { T::Offset::from_usize(0).unwrap_unchecked() });
    }

    for (indices, array) in indices.iter().zip(arrays.iter()) {
        for (i, dst) in indices.iter().enumerate() {
            unsafe {
                let (offset, data) = out.get_unchecked_mut(*dst as usize);
                let raw: &[u8] = array.value_unchecked(i).as_ref();
                let pos = offset.len();
                offset.set_len(pos + 1);
                *offset.get_unchecked_mut(pos) = *offset.get_unchecked(pos - 1)
                    + T::Offset::from_usize(raw.len()).unwrap_unchecked();
                data.extend_from_slice(raw);
            }
        }
    }

    let nulls = if needs_scatter_nulls(arrays) {
        scatter_nulls_arr(indices, hist, arrays)
    } else {
        vec![None; hist.len()]
    };

    out.into_iter()
        .zip(nulls.into_iter())
        .map(|((offsets, data), nulls)| unsafe {
            let offsets = OffsetBuffer::new_unchecked(offsets.into());
            Arc::new(GenericByteArray::<T>::new_unchecked(
                offsets,
                data.into(),
                nulls,
            )) as _
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

fn scatter_boolean(
    indices: &[Vec<u64>],
    hist: &[usize],
    arrays: &[&BooleanArray],
) -> Vec<ArrayRef> {
    let mut out: Vec<_> = hist.iter().map(|l| BooleanBufferBuilder::new(*l)).collect();

    for (indices, array) in indices.into_iter().zip(arrays.into_iter()) {
        let values = array.values();
        for (i, dst) in indices.iter().enumerate() {
            out[*dst as usize].append(values.value(i));
        }
    }

    let nulls = if needs_scatter_nulls(arrays) {
        scatter_nulls_arr(indices, hist, arrays)
    } else {
        vec![None; hist.len()]
    };

    out.into_iter()
        .zip(nulls)
        .map(|(mut out, nulls)| {
            let values = out.finish();
            Arc::new(BooleanArray::new(values, nulls)) as _
        })
        .collect()
}
