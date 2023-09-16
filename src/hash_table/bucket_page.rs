use std::mem::size_of;

use tokio::sync::RwLockWriteGuard;

use crate::{
    bitmap::BitMap,
    copy_bytes, get_bytes,
    page::{PageInner, PAGE_SIZE},
    pair::Pair,
    put_bytes,
    storable::Storable,
};

pub const DEFAULT_BIT_SIZE: usize = 512 / 8;
pub const VALUES_START: usize = DEFAULT_BIT_SIZE * 2;

pub struct Bucket<K, V, const BIT_SIZE: usize = DEFAULT_BIT_SIZE> {
    pub occupied: BitMap<BIT_SIZE>,
    pub readable: BitMap<BIT_SIZE>,
    pairs: [Option<Pair<K, V>>; 512],
}

impl<'a, const BIT_SIZE: usize, K, V> Bucket<K, V, BIT_SIZE>
where
    K: Storable + Copy + Eq,
    V: Storable + Copy + Eq,
{
    pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
        let mut occupied = BitMap::<BIT_SIZE>::new();
        copy_bytes!(occupied.as_mut_slice(), data, 0, BIT_SIZE);

        let mut readable = BitMap::<BIT_SIZE>::new();
        copy_bytes!(readable.as_mut_slice(), data, BIT_SIZE, BIT_SIZE);

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();

        // Use the occupied map to find pairs to insert
        let mut pairs: [Option<Pair<K, V>>; 512] = std::array::from_fn(|_| None);

        let size = BIT_SIZE * 8;
        let mut pos = BIT_SIZE * 2;
        for (i, pair) in pairs.iter_mut().enumerate().take(size) {
            if !occupied.check(i) {
                continue;
            }

            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_size;
            let v_bytes = get_bytes!(data, pos, v_size);
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

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, PageInner>) {
        put_bytes!(page.data, self.occupied.as_slice(), 0, BIT_SIZE);
        put_bytes!(page.data, self.readable.as_slice(), BIT_SIZE, BIT_SIZE);

        let mut pos = BIT_SIZE * 2;
        let p_size = size_of::<K>() + size_of::<V>();
        for pair in &self.pairs {
            if pos + p_size > PAGE_SIZE {
                break;
            }

            if let Some(pair) = pair {
                pair.a.write_to(&mut page.data, pos);
                pos += pair.a.size();
                pair.b.write_to(&mut page.data, pos);
                pos += pair.b.size();
            }
        }
    }

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

    pub fn get_at(&self, i: usize) -> Option<Pair<K, V>> {
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
        page::Page,
    };

    #[tokio::test]
    async fn test_bucket() {
        let page = Page::default();
        let mut page_w = page.write().await;

        let mut bucket: Bucket<i32, i32, DEFAULT_BIT_SIZE> = Bucket::new(&page_w.data);

        bucket.insert(&1, &2);
        bucket.insert(&3, &4);
        bucket.insert(&5, &6);
        bucket.insert(&7, &8);
        bucket.remove(&7, &8);

        assert!(bucket.get_at(0).unwrap() == (1, 2));
        assert!(bucket.get_at(1).unwrap() == (3, 4));
        assert!(bucket.get_at(2).unwrap() == (5, 6));
        assert!(bucket.get_at(3).is_none());

        bucket.write_data(&mut page_w);

        drop(bucket);

        // Make sure it reads back ok
        let bucket: Bucket<i32, i32, DEFAULT_BIT_SIZE> = Bucket::new(&page_w.data);
        assert!(bucket.get_at(0).unwrap() == (1, 2));
        assert!(bucket.get_at(1).unwrap() == (3, 4));
        assert!(bucket.get_at(2).unwrap() == (5, 6));
        assert!(bucket.get_at(3).is_none());

        let find1 = bucket.find(&1);
        assert!(find1.len() == 1);
        assert!(find1[0] == 2);
    }
}
