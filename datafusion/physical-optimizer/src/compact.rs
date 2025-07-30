use datafusion_common::{
    config::ConfigOptions,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result,
};
use datafusion_expr::JoinType;
use datafusion_physical_plan::{
    aggregates::AggregateExec, coalesce_batches::CoalesceBatchesExec,
    compact::CompactExec, execution_plan::CardinalityEffect, joins::HashJoinExec,
    repartition::RepartitionExec, sorts::sort::SortExec, ExecutionPlan,
};

use crate::PhysicalOptimizerRule;

use std::sync::Arc;

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default, Debug)]
pub struct CompactBatches {}

impl CompactBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}
impl PhysicalOptimizerRule for CompactBatches {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.execution.compact_batches {
            return Ok(plan);
        }

        fn passthrough(root: &dyn ExecutionPlan) -> bool {
            let root_any = root.as_any();

            return root_any.downcast_ref::<RepartitionExec>().is_some()
                || root_any.downcast_ref::<CoalesceBatchesExec>().is_some()
                || matches!(root.cardinality_effect(), CardinalityEffect::Equal)
                || root.children().len() == 0;
        }

        fn rec(
            root: Arc<dyn ExecutionPlan>,
            compact_needed: bool,
            compact_threshold: f32,
        ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
            // todo: Move compaction into hash join: Different compaction strategy for build / probe side columns
            // for now we assume that the join reduces cardinality -- so it is better to compact after the join
            if let Some(hj) = root.as_any().downcast_ref::<HashJoinExec>() {
                let left = rec(Arc::clone(&hj.left()), true, compact_threshold)?; // build side
                let right = rec(Arc::clone(&hj.right()), false, compact_threshold)?; // probe side

                return if left.transformed || right.transformed || compact_needed {
                    let new_root = root.with_new_children(vec![left.data, right.data])?;
                    let new_root = if compact_needed {
                        Arc::new(CompactExec::new(compact_threshold, new_root))
                    } else {
                        new_root
                    };
                    Ok(Transformed::yes(new_root))
                } else {
                    Ok(Transformed::no(root))
                };
            }

            if let Some(agg) = root.as_any().downcast_ref::<AggregateExec>() {
                let input = rec(Arc::clone(&agg.input()), true, compact_threshold)?;

                return if input.transformed {
                    Ok(Transformed::yes(root.with_new_children(vec![input.data])?))
                } else {
                    Ok(Transformed::no(root))
                };
            }

            if let Some(sort) = root.as_any().downcast_ref::<SortExec>() {
                let input = rec(Arc::clone(&sort.input()), true, compact_threshold)?;

                return if input.transformed {
                    Ok(Transformed::yes(root.with_new_children(vec![input.data])?))
                } else {
                    Ok(Transformed::no(root))
                };
            }

            if passthrough(root.as_ref()) {
                return root
                    .map_children(|child| rec(child, compact_needed, compact_threshold));
            }

            if root.as_any().downcast_ref::<CompactExec>().is_some() {
                return root.map_children(|child| rec(child, false, compact_threshold));
            }

            let new_root =
                root.map_children(|child| rec(child, false, compact_threshold))?;

            if compact_needed {
                Ok(Transformed::yes(Arc::new(CompactExec::new(
                    compact_threshold,
                    new_root.data,
                ))))
            } else {
                Ok(new_root)
            }
        }

        rec(plan, false, config.execution.compact_threshold as f32).data()
    }

    fn name(&self) -> &str {
        "compact"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
