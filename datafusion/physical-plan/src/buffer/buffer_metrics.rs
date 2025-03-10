use crate::metrics::ExecutionPlanMetricsSet;

use super::PartitionMetrics;

#[derive(Clone, Debug)]
pub(crate) struct BufferMetrics {
    pub(crate) unpartitioned: PartitionMetrics,
    pub(crate) partitioned: PartitionMetrics,
    pub(crate) spilled: PartitionMetrics,
}

impl BufferMetrics {
    pub(crate) fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let unpartitioned =
            PartitionMetrics::register("unparittioned", partition, metrics);
        let partitioned = PartitionMetrics::register("partitioned", partition, metrics);
        let spilled = PartitionMetrics::register("spilled", partition, metrics);

        Self {
            unpartitioned,
            partitioned,
            spilled,
        }
    }
}
