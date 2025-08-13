use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, AsArray, ByteView, GenericByteViewArray, NullBufferBuilder,
        RecordBatchOptions,
    },
    datatypes::{BinaryViewType, ByteViewType, DataType, SchemaRef, StringViewType},
    error::ArrowError,
    record_batch::RecordBatch,
};

/// concat_reduce_views delegates to [`arrow::compute::concat`] except for
/// StringView/BinaryView columns. In those cases, it only creates references
/// to buffers that are actually referenced in the newly-created array.
pub fn concat_reduce_views(arrays: &[&dyn Array]) -> Result<ArrayRef, ArrowError> {
    if arrays.is_empty() {
        return Err(ArrowError::ComputeError(
            "concat requires input of at least one array".to_string(),
        ));
    }

    match arrays[0].data_type() {
        DataType::Utf8View => Ok(concat_reduce_views_impl::<StringViewType>(arrays)),
        DataType::BinaryView => Ok(concat_reduce_views_impl::<BinaryViewType>(arrays)),
        _ => arrow::compute::concat(arrays),
    }
}

pub fn concat_batches_reduce_views<'a>(
    schema: &SchemaRef,
    input_batches: impl IntoIterator<Item = &'a RecordBatch>,
) -> Result<RecordBatch, ArrowError> {
    // When schema is empty, sum the number of the rows of all batches
    if schema.fields().is_empty() {
        let num_rows: usize = input_batches.into_iter().map(RecordBatch::num_rows).sum();
        let mut options = RecordBatchOptions::default();
        options.row_count = Some(num_rows);
        return RecordBatch::try_new_with_options(schema.clone(), vec![], &options);
    }

    let batches: Vec<&RecordBatch> = input_batches.into_iter().collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = concat_reduce_views(
            &batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect::<Vec<_>>(),
        )?;
        arrays.push(array);
    }

    RecordBatch::try_new(schema.clone(), arrays)
}

fn concat_reduce_views_impl<V: ByteViewType>(arrays: &[&dyn Array]) -> ArrayRef {
    let total: usize = arrays.iter().map(|a| a.len()).sum();
    let buffers: usize = arrays
        .iter()
        .map(|a| a.as_string_view().data_buffers().len())
        .sum();
    let mut views = Vec::with_capacity(total);
    let mut buffers = Vec::with_capacity(buffers);
    let mut buffer_dedup = HashMap::new();
    let mut nulls = NullBufferBuilder::new(total);

    for array in arrays {
        let array = array.as_byte_view::<V>();
        if let Some(anulls) = array.nulls() {
            nulls.append_buffer(anulls);
        }

        let adjust_views = buffers.len() > 0 && array.data_buffers().len() > 0;

        if adjust_views {
            match array.nulls().filter(|n| n.null_count() > 0) {
                Some(anulls) => {
                    views.extend(array.views().iter().enumerate().map(|(idx, view)| {
                        let mut byte_view = ByteView::from(*view);
                        if byte_view.length > 12 && anulls.is_valid(idx) {
                            let buffer_idx = byte_view.buffer_index as usize;
                            let buffer =
                                unsafe { array.data_buffers().get_unchecked(buffer_idx) };
                            match buffer_dedup.entry(buffer.as_ptr()) {
                                Entry::Occupied(occupied_entry) => {
                                    byte_view.buffer_index = *occupied_entry.get() as u32;
                                }
                                Entry::Vacant(vacant_entry) => {
                                    byte_view.buffer_index = buffers.len() as u32;
                                    vacant_entry.insert(buffers.len());
                                    buffers.push(buffer.clone());
                                }
                            }
                            byte_view.as_u128()
                        } else {
                            *view
                        }
                    }));
                }
                None => {
                    views.extend(array.views().iter().map(|view| {
                        let mut byte_view = ByteView::from(*view);
                        if byte_view.length > 12 {
                            let buffer_idx = byte_view.buffer_index as usize;
                            let buffer =
                                unsafe { array.data_buffers().get_unchecked(buffer_idx) };
                            match buffer_dedup.entry(buffer.as_ptr()) {
                                Entry::Occupied(occupied_entry) => {
                                    byte_view.buffer_index = *occupied_entry.get() as u32;
                                }
                                Entry::Vacant(vacant_entry) => {
                                    byte_view.buffer_index = buffers.len() as u32;
                                    vacant_entry.insert(buffers.len());
                                    buffers.push(buffer.clone());
                                }
                            }
                            byte_view.as_u128()
                        } else {
                            *view
                        }
                    }));
                }
            }
        } else {
            buffers.extend(array.data_buffers().iter().cloned());
            views.extend_from_slice(array.views());
        }
    }

    Arc::new(unsafe {
        GenericByteViewArray::<V>::new_unchecked(views.into(), buffers, nulls.finish())
    })
}
