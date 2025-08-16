use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion_arrow_extra::compute;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExprRef;

/// Partitions n batches into n partitions with the given keys and hash seed
///
/// maintains internal buffers to avoid allocations
pub struct Partitioner {
    key: Box<[PhysicalExprRef]>,
    random_state: ahash::RandomState,
    hash_buffer: Vec<Vec<u64>>,
    histogram: Vec<usize>,
}

impl Partitioner {
    pub fn new(
        key: impl Into<Box<[PhysicalExprRef]>>,
        random_state: ahash::RandomState,
        num_partitions: usize,
    ) -> Self {
        Self {
            key: key.into(),
            random_state,
            hash_buffer: vec![vec![]; num_partitions],
            histogram: vec![0; num_partitions],
        }
    }

    pub fn partition(&mut self, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
        partition(
            &self.key,
            batches,
            &self.random_state,
            &mut self.hash_buffer,
            &mut self.histogram,
        )
    }
}

fn partition(
    key: &[PhysicalExprRef],
    batches: &[RecordBatch],
    random_state: &ahash::RandomState,
    hash_buffer: &mut [Vec<u64>],
    histogram: &mut [usize],
) -> Result<Vec<RecordBatch>> {
    assert!(!batches.is_empty());

    let num_partitions = histogram.len();
    let schema = batches[0].schema();
    let num_columns = batches[0].num_columns();
    let num_batches = batches.len();

    histogram.fill(0);
    let mut keys = Vec::new();
    for (bi, batch) in batches.iter().enumerate() {
        keys.clear();
        for expr in key {
            keys.push(expr.evaluate(batch)?.into_array(batch.num_rows())?);
        }

        hash_buffer[bi].resize(batch.num_rows(), 0);
        create_hashes(&keys, &random_state, &mut hash_buffer[bi])?;

        if num_partitions.is_power_of_two() {
            for hash in hash_buffer[bi].iter_mut() {
                // modulo bit trick: a % (2^k) == a & (2^k - 1)
                *hash &= num_partitions as u64 - 1;
                histogram[*hash as usize] += 1;
            }
        } else {
            for hash in hash_buffer[bi].iter_mut() {
                *hash %= num_partitions as u64;
                histogram[*hash as usize] += 1;
            }
        }
    }

    let mut out_columns: Vec<_> = (0..num_partitions)
        .map(|_| Vec::with_capacity(num_columns))
        .collect();

    let mut col_refs = Vec::with_capacity(num_batches);
    for col in 0..num_columns {
        for batch in batches {
            col_refs.push(batch.column(col).as_ref());
        }

        let cols = compute::scatter(&hash_buffer[..batches.len()], &histogram, &col_refs);

        for (i, col) in cols.into_iter().enumerate() {
            out_columns[i].push(col)
        }

        col_refs.clear();
    }

    Ok(out_columns
        .into_iter()
        .enumerate()
        .map(|(i, cols)| unsafe {
            RecordBatch::new_unchecked(Arc::clone(&schema), cols, histogram[i])
        })
        .collect::<Vec<_>>())
}
