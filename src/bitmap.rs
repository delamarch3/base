pub struct BitMap<const SIZE: usize> {
    inner: [u8; SIZE],
}

impl<const SIZE: usize> BitMap<SIZE> {
    pub fn new() -> Self {
        Self { inner: [0; SIZE] }
    }

    pub fn set(&mut self, i: usize, val: bool) {
        let pos_i = i / 8;
        let pos_j = i % 8;

        let b = &mut self.inner[pos_i];

        if val {
            *b |= 1 << pos_j;
        } else {
            *b &= !(1 << pos_j);
        }
    }

    pub fn check(&self, i: usize) -> bool {
        let pos_i = i / 8;
        let pos_j = i % 8;

        let b = self.inner[pos_i];

        if (1 << pos_j) & b > 0 {
            true
        } else {
            false
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner
    }
}

#[cfg(test)]
mod test {
    use super::BitMap;

    #[test]
    fn test_bitmap() {
        let mut bm = BitMap::<128>::new();

        bm.set(0, true);
        bm.set(1, true);
        bm.set(8, true);
        bm.set(177, true);
        bm.set(200, true);
        bm.set(512, true);

        assert!(bm.check(0));
        assert!(bm.check(1));
        assert!(bm.check(8));
        assert!(bm.check(177));
        assert!(bm.check(200));
        assert!(bm.check(512));
    }
}
