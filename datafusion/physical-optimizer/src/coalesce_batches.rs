// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! CoalesceBatches optimizer that groups batches together rows
//! in bigger batches to avoid overhead with small batches

use crate::PhysicalOptimizerRule;

use std::{any::Any, sync::Arc};

use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::{
    coalesce_batches::CoalesceBatchesExec, compact::CompactExec, filter::FilterExec,
    joins::HashJoinExec, repartition::RepartitionExec, ExecutionPlan,
};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default, Debug)]
pub struct CoalesceBatches {}

impl CoalesceBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

enum Strategy {
    Full,
    Reduced,
}

// The goal here is to detect operators that could produce small batches and only
// wrap those ones with a CoalesceBatchesExec operator. An alternate approach here
// would be to build the coalescing logic directly into the operators
// See https://github.com/apache/datafusion/issues/139
fn coalesce_strategy(plan_any: &dyn Any) -> Option<Strategy> {
    if plan_any.downcast_ref::<FilterExec>().is_some()
        || plan_any.downcast_ref::<HashJoinExec>().is_some()
    {
        return Some(Strategy::Full);
    }

    if plan_any
        .downcast_ref::<RepartitionExec>()
        .map(|repart| matches!(repart.partitioning(), Partitioning::Hash(_, _)))
        .unwrap_or(false)
    {
        return Some(Strategy::Reduced);
    }
    return None;
}

impl PhysicalOptimizerRule for CoalesceBatches {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }

        let target_batch_size = config.execution.batch_size;
        plan.transform_up(|plan| {
            let strategy = coalesce_strategy(plan.as_any());

            if let Some(strategy) = strategy {
                let threshold = match strategy {
                    Strategy::Full => target_batch_size,
                    Strategy::Reduced => target_batch_size / 2,
                };

                if config.execution.compact_batches {
                    Ok(Transformed::yes(Arc::new(CoalesceBatchesExec::new(
                        plan, threshold,
                    ))))
                } else {
                    Ok(Transformed::yes(Arc::new(CoalesceBatchesExec::new(
                        Arc::new(CompactExec::new(
                            config.execution.compact_threshold as f32,
                            plan,
                        )) as _,
                        threshold,
                    ))))
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
