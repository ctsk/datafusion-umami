use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::assert_batches_sorted_eq;
use datafusion_common::record_batch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{binary, col};

use futures::TryStreamExt;

use crate::common::collect;
use crate::joins::test_utils::compare_batches;
use crate::metrics::ExecutionPlanMetricsSet;
use crate::umami::buffer::AdaptiveBuffer;
use crate::umami::buffer::AdaptiveSinkConfig;
use crate::umami::buffer::IoUringSpillBuffer;
use crate::umami::buffer::LazyPartitionBuffer;
use crate::umami::buffer::LazyPartitionedSource;
use crate::umami::buffer::PartitionIdx;
use crate::umami::buffer::PartitionMemoryBuffer;
use crate::umami::buffer::PartitionedSource;
use crate::umami::buffer::Sink;
use crate::umami::buffer::SpillBuffer;
use crate::umami::wrapper::InputKind;
use crate::utils::RowExpr;
use crate::{
    projection::ProjectionExec,
    test::TestMemoryExec,
    umami::{buffer::MemoryBuffer, wrapper::MaterializeWrapper},
    ExecutionPlan,
};

trait BufferCreator {
    type Buf: LazyPartitionBuffer + Send + 'static;
    fn new(schema: SchemaRef, ctx: Arc<TaskContext>) -> Self::Buf;
    fn post_sink(_buf: &<Self::Buf as LazyPartitionBuffer>::Sink) {}
}

async fn test_buffer_generic<T: BufferCreator>() -> Result<()> {
    let task_ctx = Arc::new(TaskContext::default());
    let batch = record_batch!(("nums", Int32, vec![Some(1), Some(10), Some(100)]))?;
    let schema = batch.schema();
    let input =
        TestMemoryExec::try_new_exec(&[vec![batch; 3]], Arc::clone(&schema), None)?;
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
    let buf = T::new(Arc::clone(&schema), Arc::clone(&task_ctx));
    let wrapped = MaterializeWrapper::new(factory, input, 0, task_ctx, buf);
    let batches = collect(wrapped.stream()).await?;

    assert_batches_sorted_eq!(
        [
            "+--------+",
            "| square |",
            "+--------+",
            "| 1      |",
            "| 1      |",
            "| 1      |",
            "| 100    |",
            "| 100    |",
            "| 100    |",
            "| 10000  |",
            "| 10000  |",
            "| 10000  |",
            "+--------+",
        ],
        &batches
    );

    Ok(())
}

async fn roundtrip<T: BufferCreator>(data: Vec<RecordBatch>) -> Result<()> {
    let schema = data[0].schema();
    let task_ctx = Arc::new(TaskContext::default());
    let mut buf = T::new(Arc::clone(&schema), task_ctx);
    let mut sink = buf.make_sink(Arc::clone(&schema))?;
    for batch in data.iter() {
        sink.push(batch.clone()).await?;
    }
    T::post_sink(&sink);
    let mut source = buf.make_source(sink).await?;

    let mut out = Vec::new();
    let mut unpartitioned: Vec<RecordBatch> =
        source.unpartitioned().await?.try_collect().await?;
    out.append(&mut unpartitioned);

    let mut source = source.into_partitioned();
    for p in 0..buf.partition_count() {
        let stream = source.stream_partition(PartitionIdx(p)).await;
        let mut batches: Vec<_> = stream.try_collect().await?;
        out.append(&mut batches);
    }

    compare_batches(&data, &out);

    Ok(())
}

#[tokio::test]
async fn test_buffer_mem() -> Result<()> {
    struct BC {}
    impl BufferCreator for BC {
        type Buf = MemoryBuffer;
        fn new(_schema: SchemaRef, _ctx: Arc<TaskContext>) -> Self::Buf {
            MemoryBuffer::default()
        }
    }
    test_buffer_generic::<BC>().await
}

#[tokio::test]
async fn test_buffer_spill() -> Result<()> {
    struct BC {}
    impl BufferCreator for BC {
        type Buf = SpillBuffer;
        fn new(_schema: SchemaRef, ctx: Arc<TaskContext>) -> Self::Buf {
            let dummy_metrics = ExecutionPlanMetricsSet::new();
            SpillBuffer::new(ctx.runtime_env(), dummy_metrics)
        }
    }
    test_buffer_generic::<BC>().await
}

#[tokio::test]
async fn test_buffer_mem_partition() -> Result<()> {
    struct BC {}
    impl BufferCreator for BC {
        type Buf = PartitionMemoryBuffer;
        fn new(schema: SchemaRef, _ctx: Arc<TaskContext>) -> Self::Buf {
            let name = schema.field(0).name();
            let keys: RowExpr = [col(name, &schema).unwrap()].into_iter().collect();
            PartitionMemoryBuffer::new_random(keys, 4)
        }
    }
    test_buffer_generic::<BC>().await
}

#[tokio::test]
async fn test_adaptive_buffer() -> Result<()> {
    struct BC {}
    impl BufferCreator for BC {
        type Buf = AdaptiveBuffer;
        fn new(schema: SchemaRef, _ctx: Arc<TaskContext>) -> Self::Buf {
            let name = schema.field(0).name();
            let keys: RowExpr = [col(name, &schema).unwrap()].into_iter().collect();
            let config = AdaptiveSinkConfig { partition_start: 1 };
            AdaptiveBuffer::builder()
                .key_expr(keys)
                .num_partitions(4)
                .schema(schema)
                .sink_config(config)
                .build()
        }
    }
    test_buffer_generic::<BC>().await?;
    let rb = record_batch!(
        ("num", Int32, Vec::from_iter(0..100)),
        (
            "val",
            Utf8,
            Vec::from_iter((0..100).map(|v| format!("value_{v}")))
        )
    )?;
    let rb = vec![rb; 10];
    roundtrip::<BC>(rb).await
}

#[tokio::test]
async fn test_buffer_uring() -> Result<()> {
    struct BC {}
    impl BufferCreator for BC {
        type Buf = IoUringSpillBuffer;
        fn new(_schema: SchemaRef, ctx: Arc<TaskContext>) -> Self::Buf {
            IoUringSpillBuffer::new(ctx.runtime_env())
        }
    }
    test_buffer_generic::<BC>().await?;
    let rb = record_batch!(
        ("num", Int32, Vec::from_iter(0..100)),
        (
            "val",
            Utf8,
            Vec::from_iter((0..100).map(|v| format!("value_{v}")))
        )
    )?;
    let rb = vec![rb; 10];
    roundtrip::<BC>(rb).await
}
