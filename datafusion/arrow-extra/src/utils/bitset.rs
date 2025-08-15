use fixedbitset::FixedBitSet;

pub trait AdHocBitSet {
    fn capacity(&self) -> usize;
    fn clear(&mut self);
    unsafe fn set_bit(&mut self, idx: usize, value: bool);
    unsafe fn get_bit(&self, idx: usize) -> bool;
}

impl AdHocBitSet for u64 {
    fn capacity(&self) -> usize {
        64
    }

    fn clear(&mut self) {
        *self = 0;
    }

    unsafe fn set_bit(&mut self, idx: usize, value: bool) {
        *self |= (value as u64) << idx;
    }

    unsafe fn get_bit(&self, idx: usize) -> bool {
        ((*self >> idx) & 1) != 0
    }
}

impl AdHocBitSet for FixedBitSet {
    fn capacity(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear();
    }

    unsafe fn set_bit(&mut self, idx: usize, value: bool) {
        self.set_unchecked(idx, value);
    }

    unsafe fn get_bit(&self, idx: usize) -> bool {
        self.contains_unchecked(idx)
    }
}
