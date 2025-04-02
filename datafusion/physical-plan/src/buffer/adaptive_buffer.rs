use std::{sync::Arc, usize};

use crate::{common::IPCWriter, metrics, repartition::BatchPartitioner};
use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::{config::ExecutionOptions, error::Result};
use datafusion_execution::{
    disk_manager::RefCountedTempFile, runtime_env::RuntimeEnv, SendableRecordBatchStream,
};
use datafusion_physical_expr::{Partitioning, PhysicalExprRef};
use futures::StreamExt;

use super::{
    buffer_metrics::BufferMetrics, MaterializedBatches, MaterializedPartition,
    PartitionMetrics,
};

//
//  TRANSITIONS
//    STANDARD
//       \   Number of bytes? tuples? Greater than threshold
//       \
//       v
//   PARTITION
//       \   Number of bytes? tuples? in memory
//       \
//       v
//     SPILL

const SPILL_ID: &'static str = "umami";

#[derive(Clone, Debug)]
pub struct AdaptiveBufferOptions {
    _page_grow_factor: usize,
    partition_value: usize,
    partition_threshold: usize,
    spill_threshold: usize,
    start_partitioned: bool,
}

impl Default for AdaptiveBufferOptions {
    fn default() -> Self {
        Self {
            _page_grow_factor: 2,
            partition_value: 256,
            partition_threshold: 0,
            spill_threshold: 1 << 20,
            start_partitioned: false,
        }
    }

}

impl AdaptiveBufferOptions {
    pub fn with_partition_value(mut self, partition_value: usize) -> Self {
        self.partition_value = partition_value;
        self
    }

    pub fn with_partition_threshold(mut self, partition_threshold: usize) -> Self {
        self.partition_threshold = partition_threshold;
        self
    }

    pub fn with_spill_threshold(mut self, spill_threshold: usize) -> Self {
        self.spill_threshold = spill_threshold;
        self
    }

    pub fn with_start_partitioned(mut self, start_partitioned: bool) -> Self {
        self.start_partitioned = start_partitioned;
        self
    }
}

impl AdaptiveBufferOptions {
    pub(crate) fn from_config(config: &ExecutionOptions) -> Self {
        if config.adaptive_buffer_enabled {
            Self {
                _page_grow_factor: 2,
                partition_value: config.adaptive_buffer_num_partitions,
                partition_threshold: config.adaptive_buffer_partition_threshold,
                spill_threshold: config.adaptive_buffer_spill_threshold,
                start_partitioned: config.adaptive_buffer_start_partitioned
            }
        } else {
            Self::passive()
        }
    }

    pub fn passive() -> Self {
        Self::default()
            .with_partition_threshold(usize::MAX)
            .with_spill_threshold(usize::MAX)
            .with_start_partitioned(false)
    }

    pub fn always_partition() -> Self {
        Self::default()
            .with_partition_threshold(0)
            .with_spill_threshold(usize::MAX)
            .with_start_partitioned(true)
    }
}

enum PartitionState {
    InMemory {
        batches: Vec<RecordBatch>,
    },
    Spilling {
        file: RefCountedTempFile,
        writer: IPCWriter,
    },
}

impl PartialEq for PartitionState {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::InMemory { batches: l_batches },
                Self::InMemory { batches: r_batches },
            ) => l_batches == r_batches,
            _ => false,
        }
    }
}

impl Default for PartitionState {
    fn default() -> Self {
        Self::InMemory { batches: vec![] }
    }
}

impl std::fmt::Debug for PartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartitionState::InMemory { batches } => {
                write!(f, "InMemory({:?})", batches)
            }
            PartitionState::Spilling { writer, file: _ } => {
                write!(f, "Spilling({})", writer.path.display())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct Partition {
    id: usize,
    metrics: PartitionMetrics,
    spilled_metrics: PartitionMetrics,
    state: PartitionState,
}

impl Partition {
    fn new(id: usize) -> Self {
        Self {
            id,
            metrics: Default::default(),
            spilled_metrics: Default::default(),
            state: Default::default(),
        }
    }

    fn spilled(&self) -> bool {
        match &self.state {
            PartitionState::InMemory { batches: _ } => false,
            PartitionState::Spilling { writer: _, file: _ } => true,
        }
    }

    fn spill(&mut self, schema: &Schema, runtime: &RuntimeEnv) -> Result<()> {
        match &self.state {
            PartitionState::InMemory { batches } => {
                let spill_file = runtime.disk_manager.create_tmp_file(&SPILL_ID)?;
                let mut writer = IPCWriter::new(spill_file.path(), schema)?;

                for batch in batches {
                    self.metrics.retract(&batch);
                    self.spilled_metrics.update(&batch);
                    writer.write(&batch)?;
                }

                self.state = PartitionState::Spilling {
                    writer,
                    file: spill_file,
                };

                Ok(())
            }
            PartitionState::Spilling { writer: _, file: _ } => {
                panic!("Can not spill a spilled partition");
            }
        }
    }

    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        match &mut self.state {
            PartitionState::InMemory { batches } => {
                self.metrics.update(&batch);
                batches.push(batch);
                Ok(())
            }
            PartitionState::Spilling { writer, file: _ } => {
                self.spilled_metrics.update(&batch);
                writer.write(&batch)
            }
        }
    }

    fn finalize(self) -> Result<MaterializedPartition> {
        match self.state {
            PartitionState::InMemory { batches } => {
                Ok(MaterializedPartition::InMemory {
                    batches,
                    _metrics: self.metrics,
                })
                // let batches = std::mem::take(batches);
                // self.metrics = Default::default();
                // self.state = Default::default();
                // Ok(batches)
            }
            PartitionState::Spilling { mut writer, file } => {
                writer.finish()?;

                Ok(MaterializedPartition::Spilled {
                    file,
                    _metrics: self.spilled_metrics,
                })
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum State {
    Standard {
        batches: Vec<RecordBatch>,
    },
    Partitioned {
        unpartitioned: Vec<RecordBatch>,
        partitions: Vec<Partition>,
    },
}

pub(crate) struct AdaptiveBuffer {
    options: AdaptiveBufferOptions,
    partitioner: BatchPartitioner,
    runtime: Arc<RuntimeEnv>,
    schema: SchemaRef,

    state: State,
    totals: PartitionMetrics,
    totals_spilled: PartitionMetrics,
}

impl std::fmt::Debug for AdaptiveBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AdaptiveBuffer(
        options={:?},
        state={:?},
        totals={:?},
        totals_spilled={:?}
)",
            self.options, self.state, self.totals, self.totals_spilled,
        )
    }
}

impl AdaptiveBuffer {
    pub(crate) fn try_new(
        expr: Vec<PhysicalExprRef>,
        runtime: Arc<RuntimeEnv>,
        schema: SchemaRef,
        options: Option<AdaptiveBufferOptions>,
    ) -> Result<Self> {
        let options = options.unwrap_or_default();

        let partitioner = BatchPartitioner::try_new(
            Partitioning::Hash(expr, options.partition_value),
            metrics::Time::new(),
        )?;

        let state = Self::init_state(&options);
        Ok(Self {
            options,
            state,
            partitioner,
            runtime,
            schema,
            totals: PartitionMetrics::default(),
            totals_spilled: PartitionMetrics::default(),
        })
    }

    fn init_state(abo: &AdaptiveBufferOptions) -> State {
        if abo.start_partitioned {
            State::Partitioned {
                unpartitioned: vec![],
                partitions: (0..abo.partition_value)
                    .map(|id| Partition::new(id))
                    .collect(),
            }
        } else {
            State::Standard { batches: vec![] }
        }
    }

    pub(crate) async fn _buffer(
        &mut self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<()> {
        while let Some(batch) = stream.next().await {
            self.push(batch?)?;
        }

        Ok(())
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        match &mut self.state {
            State::Standard { batches } => {
                Ok(Self::standard_push(&mut self.totals, batches, batch))
            }
            State::Partitioned {
                unpartitioned: _,
                partitions,
            } => Self::partition_push(
                &mut self.totals,
                &mut self.totals_spilled,
                &mut self.partitioner,
                partitions,
                batch,
            ),
        }?;

        self.update_state()
    }

    pub(crate) fn finalize(
        mut self,
        metrics: BufferMetrics,
    ) -> Result<MaterializedBatches> {
        match self.state {
            State::Standard { batches } => {
                metrics.unpartitioned.add(&self.totals);
                Ok(MaterializedBatches::from_unpartitioned(
                    self.schema,
                    batches,
                    self.totals,
                ))
            }
            State::Partitioned {
                unpartitioned,
                mut partitions,
            } => {
                if partitions.iter().any(|part| part.spilled()) {
                    // Partition the unpartitioned.
                    let unpartitioned = unpartitioned;

                    for batch in unpartitioned {
                        Self::partition_push(
                            &mut self.totals,
                            &mut self.totals_spilled,
                            &mut self.partitioner,
                            &mut partitions,
                            batch,
                        )?;
                    }

                    for partition in partitions.iter() {
                        metrics.partitioned.add(&partition.metrics);
                        metrics.partitioned.add(&partition.spilled_metrics);
                        metrics.spilled.add(&partition.spilled_metrics);
                    }

                    Ok(MaterializedBatches::from_partitioned(
                        self.schema,
                        partitions
                            .into_iter()
                            .map(|part| part.finalize())
                            .collect::<Result<Vec<_>>>()?,
                        self.totals,
                        self.totals_spilled,
                    ))
                } else {
                    for batch in unpartitioned.iter() {
                        metrics.unpartitioned.update(batch);
                    }

                    for partition in partitions.iter() {
                        metrics.partitioned.add(&partition.metrics);
                    }

                    Ok(MaterializedBatches::from_partially_partitioned(
                        self.schema,
                        unpartitioned,
                        partitions
                            .into_iter()
                            .map(|part| part.finalize())
                            .collect::<Result<Vec<_>>>()?,
                        self.totals,
                        self.totals_spilled,
                    ))
                }
            }
        }
    }

    fn standard_push(
        metrics: &mut PartitionMetrics,
        batches: &mut Vec<RecordBatch>,
        batch: RecordBatch,
    ) {
        metrics.update(&batch);
        batches.push(batch);
    }

    fn partition_push(
        metrics: &mut PartitionMetrics,
        metrics_spilled: &mut PartitionMetrics,
        partitioner: &mut BatchPartitioner,
        partitions: &mut Vec<Partition>,
        batch: RecordBatch,
    ) -> Result<()> {
        for item in partitioner.partition_iter(batch)? {
            let (partition, batch) = item?;
            let partition = &mut partitions[partition];

            if partition.spilled() {
                metrics_spilled.update(&batch);
            } else {
                metrics.update(&batch);
            }

            partition.push(batch)?;
        }

        Ok(())
    }

    fn update_state(&mut self) -> Result<()> {
        match &mut self.state {
            State::Standard { batches } => {
                if self.totals.num_rows.value() > self.options.spill_threshold {
                    panic!(
                        "We reached the spill threshold before partitioning. -- Can not recover."
                    )
                }
                if self.totals.num_rows.value() > self.options.partition_threshold {
                    self.state = State::Partitioned {
                        unpartitioned: std::mem::take(batches),
                        partitions: (0..self.options.partition_value)
                            .map(|id| Partition::new(id))
                            .collect(),
                    }
                }
            }
            State::Partitioned {
                unpartitioned: _,
                partitions,
            } => {
                while self.totals.num_rows.value() > self.options.spill_threshold {
                    // select the partition with the most rows
                    let partition = partitions
                        .iter_mut()
                        .max_by_key(|partition| partition.metrics.num_rows.value())
                        .unwrap();

                    self.totals.mem_used.sub(partition.metrics.mem_used.value());
                    self.totals
                        .num_batches
                        .sub(partition.metrics.num_batches.value());
                    self.totals.num_rows.sub(partition.metrics.num_rows.value());

                    self.totals_spilled
                        .mem_used
                        .add(partition.metrics.mem_used.value());
                    self.totals_spilled
                        .num_batches
                        .add(partition.metrics.num_batches.value());
                    self.totals_spilled
                        .num_rows
                        .add(partition.metrics.num_rows.value());

                    partition.spill(&self.schema, &self.runtime)?
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::expressions::col;

    use super::*;

    #[test]
    fn test_unpartitioned() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let col_b = Arc::new(Int64Array::from(vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![col_a, col_b])?;

        let options = AdaptiveBufferOptions {
            partition_threshold: 3,
            spill_threshold: 100,
            partition_value: 3,
            _page_grow_factor: 2,
            start_partitioned: false,
        };

        let runtime_env = RuntimeEnvBuilder::default().build_arc()?;

        let mut buffer = AdaptiveBuffer::try_new(
            vec![col("a", &schema)?],
            runtime_env,
            schema,
            Some(options),
        )?;

        let expected = [4, 20, 1008];

        buffer.push(batch.clone())?; // pushed unified
        println!("{:?}", buffer);
        buffer.push(batch.clone())?; // pushed
        println!("{:?}", buffer);

        assert_eq!(
            [
                buffer.totals.num_batches.value(),
                buffer.totals.num_rows.value(),
                buffer.totals.mem_used.value(),
            ],
            expected
        );

        Ok(())
    }
}
