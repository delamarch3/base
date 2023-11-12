use std::{mem::size_of, ops::Range};

use crate::{
    bitmap::BitMap,
    page::{PageBuf, PAGE_SIZE},
    pair::Pair,
    storable::Storable,
};

/// Number of bytes for the bitmaps
pub const BIT_SIZE: usize = 512 / 8;

const OCCUPIED: Range<usize> = 0..BIT_SIZE;
const READABLE: Range<usize> = BIT_SIZE..BIT_SIZE + BIT_SIZE;

pub struct Bucket<K, V> {
    pub occupied: BitMap<BIT_SIZE>,
    pub readable: BitMap<BIT_SIZE>,
    pairs: [Option<Pair<K, V>>; 512],
}

impl<K, V> From<&PageBuf> for Bucket<K, V>
where
    K: Storable,
    V: Storable,
{
    fn from(buf: &PageBuf) -> Self {
        let mut occupied = BitMap::<BIT_SIZE>::new();
        occupied.as_mut_slice().copy_from_slice(&buf[OCCUPIED]);

        let mut readable = BitMap::<BIT_SIZE>::new();
        readable.as_mut_slice().copy_from_slice(&buf[READABLE]);

        // Use the occupied map to find pairs to insert
        let mut pairs: [Option<Pair<K, V>>; 512] = std::array::from_fn(|_| None);

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();

        let mut pos = BIT_SIZE * 2;
        for (i, pair) in pairs.iter_mut().enumerate() {
            if !occupied.check(i) {
                continue;
            }

            let k_bytes = &buf[pos..pos + k_size];
            pos += k_size;

            let v_bytes = &buf[pos..pos + v_size];
            pos += v_size;

            let key = K::from_bytes(k_bytes);
            let value = V::from_bytes(v_bytes);

            *pair = Some(Pair::new(key, value));
        }

        Self {
            occupied,
            readable,
            pairs,
        }
    }
}

impl<K, V> From<&Bucket<K, V>> for PageBuf
where
    K: Storable,
    V: Storable,
{
    fn from(bucket: &Bucket<K, V>) -> Self {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[OCCUPIED].copy_from_slice(bucket.occupied.as_slice());
        ret[READABLE].copy_from_slice(bucket.occupied.as_slice());

        let mut pos = BIT_SIZE * 2;
        let p_size = size_of::<K>() + size_of::<V>();
        for pair in &bucket.pairs {
            if pos + p_size > PAGE_SIZE {
                break;
            }

            if let Some(pair) = pair {
                pair.a.write_to(&mut ret, pos);
                pos += pair.a.size();
                pair.b.write_to(&mut ret, pos);
                pos += pair.b.size();
            }
        }

        ret
    }
}

impl<K, V> From<Bucket<K, V>> for PageBuf
where
    K: Storable,
    V: Storable,
{
    fn from(bucket: Bucket<K, V>) -> Self {
        Self::from(&bucket)
    }
}

impl<K, V> Bucket<K, V>
where
    K: Storable + Copy + Eq,
    V: Storable + Copy + Eq,
{
    pub fn remove(&mut self, k: &K, v: &V) -> bool {
        let mut ret = false;
        for (i, pair) in self.pairs.iter().enumerate() {
            if let Some(pair) = pair {
                if pair.a == *k && pair.b == *v {
                    self.readable.set(i, false);
                    self.occupied.set(i, false);
                    ret = true;
                }
            }
        }

        ret
    }

    pub fn remove_at(&mut self, i: usize) {
        self.occupied.set(i, false);
        self.readable.set(i, false);
    }

    pub fn insert(&mut self, k: &K, v: &V) {
        // Find occupied
        let mut i = 0;
        loop {
            if !self.occupied.check(i) {
                break;
            }

            i += 1;
        }

        self.pairs[i] = Some(Pair::new(*k, *v));
        self.occupied.set(i, true);
        self.readable.set(i, true);
    }

    pub fn get(&self, i: usize) -> Option<Pair<K, V>> {
        if self.readable.check(i) {
            self.pairs[i]
        } else {
            None
        }
    }

    pub fn find(&self, k: &K) -> Vec<V> {
        let mut ret = Vec::new();
        for (i, pair) in self.pairs.iter().enumerate() {
            if let Some(pair) = pair {
                if pair.a == *k && self.readable.check(i) {
                    ret.push(pair.b)
                }
            }
        }

        ret
    }

    pub fn get_pairs(&self) -> Vec<Pair<K, V>> {
        let mut ret = Vec::new();
        for pair in self.pairs.iter().flatten() {
            ret.push(*pair);
        }

        ret
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        let len = self.occupied.len();
        let s = size_of::<K>() + size_of::<V>();

        if len >= (PAGE_SIZE - 128) / s {
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table::bucket_page::Bucket,
        page::{Page, PageBuf},
        writep,
    };

    #[tokio::test]
    async fn test_bucket() {
        let page = Page::default();
        let mut page_w = page.write().await;

        let mut bucket: Bucket<i32, i32> = Bucket::from(&page_w.data);

        bucket.insert(&1, &2);
        bucket.insert(&3, &4);
        bucket.insert(&5, &6);
        bucket.insert(&7, &8);
        bucket.remove(&7, &8);

        assert!(bucket.get(0).unwrap() == (1, 2));
        assert!(bucket.get(1).unwrap() == (3, 4));
        assert!(bucket.get(2).unwrap() == (5, 6));
        assert!(bucket.get(3).is_none());

        writep!(page_w, &PageBuf::from(bucket));

        // Make sure it reads back ok
        let bucket: Bucket<i32, i32> = Bucket::from(&page_w.data);
        assert!(bucket.get(0).unwrap() == (1, 2));
        assert!(bucket.get(1).unwrap() == (3, 4));
        assert!(bucket.get(2).unwrap() == (5, 6));
        assert!(bucket.get(3).is_none());

        let find1 = bucket.find(&1);
        assert!(find1.len() == 1);
        assert!(find1[0] == 2);
    }
}
