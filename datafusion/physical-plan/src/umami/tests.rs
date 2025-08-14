use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use datafusion_common::assert_batches_sorted_eq;
use datafusion_common::record_batch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{binary, col};
use futures::StreamExt;

use crate::common::collect;
use crate::joins::test_utils::compare_batches;
use crate::umami::wrapper::InputKind;
use crate::{
    projection::ProjectionExec,
    test::TestMemoryExec,
    umami::{buffer::MemoryBuffer, wrapper::MaterializeWrapper},
    ExecutionPlan,
};

#[tokio::test]
async fn test_buffer() -> Result<()> {
    let task_ctx = Arc::new(TaskContext::default());
    let batch = record_batch!(("nums", Int32, vec![Some(1), Some(10), Some(100)]))?;
    let schema = batch.schema();
    let input = TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
    let input_stream = input.execute(0, Arc::clone(&task_ctx))?;
    let input_key = col("nums", &schema)?;
    let inner = ProjectionExec::try_new(
        vec![(
            binary(
                col("nums", &schema)?,
                Operator::Multiply,
                col("nums", &schema)?,
                &schema,
            )?,
            "square".to_string(),
        )],
        input,
    )?;

    let factory = inner.execute_factory(0, Arc::clone(&task_ctx))?;
    let input = InputKind::unary(input_stream, vec![input_key]);
    let wrapped = MaterializeWrapper::<MemoryBuffer>::new(factory, input, 0, task_ctx);
    let batches = collect(wrapped.stream()).await?;

    assert_batches_sorted_eq!(
        [
            "+--------+",
            "| square |",
            "+--------+",
            "| 1      |",
            "| 100    |",
            "| 10000  |",
            "+--------+",
        ],
        &batches
    );

    Ok(())
}
