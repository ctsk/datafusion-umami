use crate::umami::buffer::PartitionIdx;

pub struct BufferReport {
    pub unpart_batches: usize,
    pub parts_in_mem: Vec<PartitionIdx>,
    pub parts_oom: Vec<PartitionIdx>,
}
