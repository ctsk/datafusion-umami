use std::{fs::File, io::BufReader, path::Path};

use arrow::{ipc::reader::FileReader, util::pretty::pretty_format_batches};
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::error::Result;
use datafusion_execution::{disk_manager::RefCountedTempFile, SendableRecordBatchStream};

use crate::{metrics, spill::read_spill_as_stream};

const BUFFER: usize = 8192;

pub(crate) mod adaptive_buffer;
pub(crate) mod memory_buffer;

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct PartitionMetrics {
    num_batches: metrics::Gauge,
    num_rows: metrics::Gauge,
    mem_used: metrics::Gauge,
}

impl PartitionMetrics {
    fn update(&mut self, batch: &RecordBatch) {
        self.num_batches.add(1);
        self.num_rows.add(batch.num_rows());
        self.mem_used.add(batch.get_array_memory_size());
    }
}

#[derive(Debug)]
pub(crate) enum MaterializedPartition {
    InMemory {
        batches: Vec<RecordBatch>,
        metrics: PartitionMetrics,
    },
    Spilled {
        file: RefCountedTempFile,
        metrics: PartitionMetrics,
    },
}

impl std::fmt::Display for MaterializedPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory { batches, .. } => {
                writeln!(f, "InMemory")?;
                if !batches.is_empty() {
                    writeln!(f, "  Batches:")?;
                    writeln!(f, "  {}", pretty_format_batches(batches).unwrap())?;
                } else {
                    writeln!(f, "  <empty>")?;
                }
            }
            Self::Spilled { file, .. } => {
                writeln!(f, "Spilled")?;
                writeln!(f, "  File: {}", file.path().display())?;
            }
        }
        Ok(())
    }
}

impl MaterializedPartition {
    pub(crate) fn is_spilled(&self) -> bool {
        matches!(self, Self::Spilled { .. })
    }
}

#[derive(Debug)]
pub(crate) struct MaterializedBatches {
    schema: SchemaRef,
    unpartitioned: Vec<RecordBatch>,
    partitions: Vec<MaterializedPartition>,
    total: PartitionMetrics,
    total_spilled: PartitionMetrics,
}

impl std::fmt::Display for MaterializedBatches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MaterializedBatches")?;
        if !self.unpartitioned.is_empty() {
            writeln!(f, "  Unpartitioned:")?;
            writeln!(f, "  {}", pretty_format_batches(&self.unpartitioned).unwrap())?;
        }
        for (i, partition) in self.partitions.iter().enumerate() {
            writeln!(f, "  Partition {}:", i)?;
            writeln!(f, "  {}", partition)?;
        }
        Ok(())
    }
}

impl MaterializedBatches {
    pub(crate) fn from_unpartitioned(
        schema: SchemaRef,
        unpartitioned: Vec<RecordBatch>,
        total: PartitionMetrics,
    ) -> Self {
        Self {
            schema,
            unpartitioned,
            partitions: vec![],
            total,
            total_spilled: PartitionMetrics::default(),
        }
    }

    pub(crate) fn from_partitioned(
        schema: SchemaRef,
        partitions: Vec<MaterializedPartition>,
        total: PartitionMetrics,
        total_spilled: PartitionMetrics,
    ) -> Self {
        Self {
            schema,
            unpartitioned: vec![],
            partitions,
            total,
            total_spilled,
        }
    }

    pub(crate) fn from_partially_partitioned(
        schema: SchemaRef,
        unpartitioned: Vec<RecordBatch>,
        partitions: Vec<MaterializedPartition>,
        total: PartitionMetrics,
        total_spilled: PartitionMetrics,
    ) -> Self {
        Self {
            schema,
            unpartitioned,
            partitions,
            total,
            total_spilled,
        }
    }

    pub(crate) fn mem_used(&self) -> usize {
        self.total.mem_used.value()
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.total.num_rows.value()
    }

    pub(crate) fn num_batches(&self) -> usize {
        self.total.num_batches.value()
    }

    pub(crate) fn spill_mask(&self) -> Option<Vec<bool>> {
        if self.partitions.is_empty() {
            None
        } else {
            Some(
                self.partitions
                    .iter()
                    .map(|part| part.is_spilled())
                    .collect(),
            )
        }
    }

    pub(crate) fn take_mem_batches(&mut self) -> Vec<RecordBatch> {
        self.unpartitioned
            .drain(..)
            .chain(self.partitions.iter_mut().flat_map(|part| match part {
                MaterializedPartition::InMemory {
                    batches,
                    metrics: _,
                } => std::mem::take(batches),
                MaterializedPartition::Spilled {
                    file: _,
                    metrics: _,
                } => {
                    vec![]
                }
            }))
            .collect()
    }

    fn read_spill_batches(path: &Path) -> Result<Vec<RecordBatch>> {
        let file = BufReader::new(File::open(path)?);
        let reader = FileReader::try_new(file, None)?;
        reader.collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()
            .map_err(|e| e.into())
    }

    pub(crate) fn take_next_spilled(
        &mut self,
    ) -> Option<(usize, Result<Vec<RecordBatch>>)> {
        let part_id = self
            .partitions
            .iter_mut()
            .position(|part| matches!(part, MaterializedPartition::Spilled { .. }))?;

        let part = std::mem::replace(
            &mut self.partitions[part_id],
            MaterializedPartition::InMemory {
                batches: vec![],
                metrics: PartitionMetrics::default(),
            },
        );

        if let MaterializedPartition::Spilled { file, metrics: _ } = part {
            Some(
                (part_id, Self::read_spill_batches(file.path()))
            )
        } else {
            unreachable!()
        }
    }

    pub(crate) fn read_spill(
        &mut self,
        id: usize,
    ) -> Option<Result<Vec<RecordBatch>>> {
        let part = std::mem::replace(
            &mut self.partitions[id],
            MaterializedPartition::InMemory {
                batches: vec![],
                metrics: PartitionMetrics::default(),
            },
        );


        if let MaterializedPartition::Spilled { file, metrics: _ } = part {
            Some(
                Self::read_spill_batches(file.path())
            )
        } else {
            unreachable!()
        }
    }


    pub(crate) fn stream_spill(
        &mut self,
        id: usize,
    ) -> Option<Result<SendableRecordBatchStream>> {
        let part = std::mem::replace(
            &mut self.partitions[id],
            MaterializedPartition::InMemory {
                batches: vec![],
                metrics: PartitionMetrics::default(),
            },
        );

        if let MaterializedPartition::Spilled { file, metrics: _ } = part {
            Some(
                read_spill_as_stream(file, self.schema.clone(), BUFFER)
            )
        } else {
            unreachable!()
        }
    }
}
