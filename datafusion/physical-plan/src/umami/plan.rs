//! An execution plan that buffers (and afterwards reads) the input stream.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};

use crate::{
    metrics::ExecutionPlanMetricsSet,
    umami::{InputKind, StreamFactory},
    utils::RowExpr,
    DisplayAs, ExecutionPlan, PlanProperties,
};

use super::StreamProvider;

#[derive(Clone, Debug)]
pub struct BufferExec {
    input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
    expr: RowExpr,
    metrics: ExecutionPlanMetricsSet,
    buffer_only: bool,
}

impl BufferExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        expr: impl Into<RowExpr>,
        buffer_only: bool,
    ) -> Self {
        let cache = input.properties().clone();
        Self {
            input,
            cache,
            expr: expr.into(),
            metrics: ExecutionPlanMetricsSet::new(),
            buffer_only,
        }
    }
}

impl DisplayAs for BufferExec {
    fn fmt_as(
        &self,
        t: crate::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        use crate::DisplayFormatType;
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BufferExec",)?;
                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for BufferExec {
    fn name(&self) -> &str {
        "BufferExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.pop().unwrap(),
            self.expr.clone(),
            self.buffer_only,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        struct Factory {
            schema: SchemaRef,
        }

        impl StreamFactory for Factory {
            fn make(
                &mut self,
                inputs: &mut dyn StreamProvider,
                _p: usize,
                _c: &TaskContext,
            ) -> Result<SendableRecordBatchStream> {
                Ok(inputs.get(0))
            }

            fn output_schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        let factory = Factory {
            schema: self.schema(),
        };

        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let input = InputKind::unary(input_stream, self.expr.clone());
        let materialized = super::apply(Box::new(factory), input, partition, context)?;
        Ok(materialized)
    }
}
