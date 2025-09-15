use crate::umami::buffer::PartitionIdx;

pub struct BufferReport {
    pub unpart_batches: usize,
    pub did_partition: bool,
    pub parts_in_mem: Vec<PartitionIdx>,
    pub parts_oom: Vec<PartitionIdx>,
}
