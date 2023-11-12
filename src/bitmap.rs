pub struct BitMap<const SIZE: usize> {
    inner: [u8; SIZE],
}

impl<const SIZE: usize> Default for BitMap<SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SIZE: usize> BitMap<SIZE> {
    pub fn new() -> Self {
        Self { inner: [0; SIZE] }
    }

    pub fn set(&mut self, i: usize, val: bool) {
        let index = i >> 3;
        let bit_index = 1 << (i & 7);

        if val {
            self.inner[index] |= bit_index;
        } else {
            self.inner[index] &= !bit_index;
        }
    }

    pub fn check(&self, i: usize) -> bool {
        let index = i >> 3;
        let bit_index = 1 << (i & 7);

        let b = self.inner[index];

        bit_index & b > 0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner
    }

    pub fn len(&self) -> usize {
        let mut ret = 0;
        for b in self.inner {
            ret += b.count_ones()
        }

        ret as usize
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

        bm.set(0, false);
        bm.set(1, false);
        bm.set(8, false);
        bm.set(177, false);
        bm.set(200, false);
        bm.set(512, false);

        assert!(!bm.check(0));
        assert!(!bm.check(1));
        assert!(!bm.check(8));
        assert!(!bm.check(177));
        assert!(!bm.check(200));
        assert!(!bm.check(512));
    }
}
