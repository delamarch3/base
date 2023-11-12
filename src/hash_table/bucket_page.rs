use std::mem::size_of;

use crate::{
    bitmap::BitMap,
    page::{PageBuf, PAGE_SIZE},
    pair::Pair,
    storable::Storable,
};

pub const DEFAULT_BIT_SIZE: usize = 512 / 8;
pub const VALUES_START: usize = DEFAULT_BIT_SIZE * 2;

pub struct Bucket<K, V, const BIT_SIZE: usize = DEFAULT_BIT_SIZE> {
    pub occupied: BitMap<BIT_SIZE>,
    pub readable: BitMap<BIT_SIZE>,
    pairs: [Option<Pair<K, V>>; 512],
}

impl<K, V, const BIT_SIZE: usize> From<&PageBuf> for Bucket<K, V, BIT_SIZE>
where
    K: Storable,
    V: Storable,
{
    fn from(buf: &PageBuf) -> Self {
        let mut occupied = BitMap::<BIT_SIZE>::new();
        occupied.as_mut_slice().copy_from_slice(&buf[0..BIT_SIZE]);

        let mut readable = BitMap::<BIT_SIZE>::new();
        readable
            .as_mut_slice()
            .copy_from_slice(&buf[BIT_SIZE..BIT_SIZE * 2]);

        // Use the occupied map to find pairs to insert
        let mut pairs: [Option<Pair<K, V>>; 512] = std::array::from_fn(|_| None);

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();

        // TODO: test_split fails if take() is removed, probably nicer to get rid of const generic
        // completely
        let mut pos = BIT_SIZE * 2;
        for (i, pair) in pairs.iter_mut().enumerate().take(BIT_SIZE * 8) {
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

impl<K, V, const BIT_SIZE: usize> From<&Bucket<K, V, BIT_SIZE>> for PageBuf
where
    K: Storable,
    V: Storable,
{
    fn from(bucket: &Bucket<K, V, BIT_SIZE>) -> Self {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[0..BIT_SIZE].copy_from_slice(bucket.occupied.as_slice());
        ret[BIT_SIZE..BIT_SIZE * 2].copy_from_slice(bucket.occupied.as_slice());

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

impl<K, V, const BIT_SIZE: usize> From<Bucket<K, V, BIT_SIZE>> for PageBuf
where
    K: Storable,
    V: Storable,
{
    fn from(bucket: Bucket<K, V, BIT_SIZE>) -> Self {
        Self::from(&bucket)
    }
}

impl<'a, const BIT_SIZE: usize, K, V> Bucket<K, V, BIT_SIZE>
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
        self.occupied.is_full()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table::bucket_page::{Bucket, DEFAULT_BIT_SIZE},
        page::{Page, PageBuf},
        writep,
    };

    #[tokio::test]
    async fn test_bucket() {
        let page = Page::default();
        let mut page_w = page.write().await;

        let mut bucket: Bucket<i32, i32, DEFAULT_BIT_SIZE> = Bucket::from(&page_w.data);

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
        let bucket: Bucket<i32, i32, DEFAULT_BIT_SIZE> = Bucket::from(&page_w.data);
        assert!(bucket.get(0).unwrap() == (1, 2));
        assert!(bucket.get(1).unwrap() == (3, 4));
        assert!(bucket.get(2).unwrap() == (5, 6));
        assert!(bucket.get(3).is_none());

        let find1 = bucket.find(&1);
        assert!(find1.len() == 1);
        assert!(find1[0] == 2);
    }
}
