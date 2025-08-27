use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, AsArray, GenericByteViewArray, NullBufferBuilder,
        RecordBatchOptions,
    },
    datatypes::{BinaryViewType, ByteViewType, DataType, SchemaRef, StringViewType},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::utils::BufferDedup;

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
    let length: usize = arrays.iter().map(|a| a.len()).sum();
    let mut views = Vec::with_capacity(length);
    let mut dedup = BufferDedup::new(arrays);
    let mut nulls = NullBufferBuilder::new(length);

    for array in arrays {
        let array = array.as_byte_view::<V>();

        if array.len() == 0 {
            continue;
        }

        if let Some(anulls) = array.nulls() {
            nulls.append_buffer(anulls);
        } else {
            nulls.append_n_non_nulls(array.len());
        }

        views.extend_from_slice(array.views());

        if array.data_buffers().len() == 0 {
            continue;
        }

        let adjust_range = views.len() - array.len()..views.len();
        let new_nulls = array.nulls().filter(|n| n.null_count() > 0);
        dedup.adjust(&mut views[adjust_range], new_nulls, array.data_buffers());
    }

    Arc::new(unsafe {
        GenericByteViewArray::<V>::new_unchecked(
            views.into(),
            dedup.finish(),
            nulls.finish(),
        )
    })
}
