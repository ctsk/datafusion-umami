use std::usize;

use arrow::{
    array::{
        Array, AsArray, GenericByteArray, GenericByteViewArray, GenericListArray,
        GenericListViewArray, OffsetSizeTrait, RecordBatch,
    },
    buffer::{Buffer, NullBuffer, ScalarBuffer},
    datatypes::{ArrowNativeType, ByteArrayType, ByteViewType, DataType},
    downcast_primitive_array,
};

fn buffer(buffer: &Buffer) -> usize {
    buffer.capacity() / buffer.strong_count()
}

fn scalar<T: ArrowNativeType>(buf: &ScalarBuffer<T>) -> usize {
    buffer(buf.inner())
}

fn nulls(nulls: Option<&NullBuffer>) -> usize {
    match nulls {
        Some(nulls) => buffer(nulls.inner().inner()),
        None => 0,
    }
}

fn byte_array<T: ByteArrayType>(array: &GenericByteArray<T>) -> usize {
    nulls(array.nulls()) + buffer(array.values()) + scalar(array.offsets().inner())
}

fn byte_view_array<T: ByteViewType>(array: &GenericByteViewArray<T>) -> usize {
    nulls(array.nulls())
        + array.data_buffers().iter().map(buffer).sum::<usize>()
        + scalar(array.views())
}

fn list_array<O: OffsetSizeTrait>(array: &GenericListArray<O>) -> usize {
    nulls(array.nulls())
        + array_size_shared(array.values())
        + scalar(array.offsets().inner())
}

fn list_view_array<O: OffsetSizeTrait>(array: &GenericListViewArray<O>) -> usize {
    nulls(array.nulls())
        + array_size_shared(array.values())
        + scalar(array.offsets())
        + scalar(array.sizes())
}

pub fn array_size_shared(array: &dyn Array) -> usize {
    downcast_primitive_array! {
        array => {
            scalar(array.values()) + nulls(array.nulls())
        },
        DataType::Utf8 => byte_array(array.as_string::<i32>()),
        DataType::LargeUtf8 => byte_array(array.as_string::<i64>()),
        DataType::Binary => byte_array(array.as_binary::<i32>()),
        DataType::LargeBinary => byte_array(array.as_binary::<i64>()),
        DataType::Utf8View => byte_view_array(array.as_string_view()),
        DataType::BinaryView => byte_view_array(array.as_binary_view()),
        DataType::FixedSizeBinary(_) => {
            nulls(array.nulls()) + buffer(array.as_fixed_size_binary().values())
        },
        DataType::List(_) => list_array(array.as_list::<i32>()),
        DataType::LargeList(_) => list_array(array.as_list::<i64>()),
        DataType::ListView(_) => list_view_array(array.as_list_view::<i32>()),
        DataType::LargeListView(_) => list_view_array(array.as_list_view::<i32>()),
        DataType::FixedSizeList(_, _) => {
            nulls(array.nulls()) + array_size_shared(array.as_fixed_size_list().values())
        }
        DataType::Struct(_) => nulls(array.nulls()) + array.as_struct().columns().iter().map(|a| array_size_shared(a)).sum::<usize>(),
        DataType::Dictionary(_, _) => {
            let da = array.as_any_dictionary();
            array_size_shared(da.values()) + array_size_shared(da.keys()) + nulls(da.nulls())
        }
        DataType::Map(_, _) => {
            let ma = array.as_map();
            array_size_shared(ma.values()) + array_size_shared(ma.keys()) + nulls(ma.nulls())
        }
        _ => {
            unimplemented!("Array size shared is not implemented for {:?}", array.data_type());
        },
    }
}

pub fn batch_size_shared(batch: &RecordBatch) -> usize {
    batch.columns().iter().map(|a| array_size_shared(a)).sum()
}
