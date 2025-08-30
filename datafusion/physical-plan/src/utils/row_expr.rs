use std::sync::Arc;

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExprRef;

#[derive(Clone, Debug)]
pub struct RowExpr {
    exprs: Arc<[PhysicalExprRef]>,
}

impl RowExpr {
    pub fn evaluate_to_array(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        let num_rows = batch.num_rows();
        self.exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|e| e.into_array(num_rows)))
            .collect()
    }
}

impl FromIterator<PhysicalExprRef> for RowExpr {
    fn from_iter<T: IntoIterator<Item = PhysicalExprRef>>(iter: T) -> Self {
        Self {
            exprs: iter.into_iter().collect(),
        }
    }
}

impl From<Vec<PhysicalExprRef>> for RowExpr {
    fn from(value: Vec<PhysicalExprRef>) -> Self {
        Self {
            exprs: value.into(),
        }
    }
}
