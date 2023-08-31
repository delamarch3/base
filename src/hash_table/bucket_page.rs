use std::mem::size_of;

use bytes::BytesMut;
use tokio::sync::RwLockWriteGuard;

use crate::{
    bitmap::BitMap,
    copy_bytes, get_bytes,
    page::{Page, DEFAULT_PAGE_SIZE},
    pair::{Pair, PairType},
    put_bytes,
};

pub const DEFAULT_BIT_SIZE: usize = 512 / 8;
pub const VALUES_START: usize = DEFAULT_BIT_SIZE * 2;

pub struct Bucket<
    K,
    V,
    const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE,
    const BIT_SIZE: usize = DEFAULT_BIT_SIZE,
> {
    pub occupied: BitMap<BIT_SIZE>,
    pub readable: BitMap<BIT_SIZE>,
    pairs: [Option<Pair<K, V>>; 512],
}

impl<'a, const PAGE_SIZE: usize, const BIT_SIZE: usize, K, V> Bucket<K, V, PAGE_SIZE, BIT_SIZE>
where
    PairType<K>: Into<BytesMut> + From<&'a [u8]> + PartialEq<K> + Copy,
    PairType<V>: Into<BytesMut> + From<&'a [u8]> + PartialEq<V> + Copy,
    K: Copy + std::fmt::Debug,
    V: Copy + std::fmt::Debug,
{
    pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
        let mut occupied = BitMap::<BIT_SIZE>::new();
        copy_bytes!(occupied.as_mut_slice(), data, 0, BIT_SIZE);

        let mut readable = BitMap::<BIT_SIZE>::new();
        copy_bytes!(readable.as_mut_slice(), data, BIT_SIZE, BIT_SIZE);

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();
        let p_size = k_size + v_size;

        // Use the occupied map to find pairs to insert
        let mut pairs = [None; 512];
        let size = BIT_SIZE * 8;

        for i in 0..size {
            if !occupied.check(i) {
                continue;
            }

            let mut pos = BIT_SIZE * 2 + p_size * i;
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_size;
            let v_bytes = get_bytes!(data, pos, v_size);

            let pair = Pair::from_bytes(k_bytes, v_bytes);
            pairs[i] = Some(pair);
        }

        Self {
            occupied,
            readable,
            pairs,
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

        let pair = Pair::new(*k, *v);
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
                    ret.push(pair.b.0)
                }
            }
        }

        ret
    }

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>) {
        put_bytes!(page.data, self.occupied.as_slice(), 0, BIT_SIZE);
        put_bytes!(page.data, self.readable.as_slice(), BIT_SIZE, BIT_SIZE);

        let mut pos = BIT_SIZE * 2;
        let size = PAGE_SIZE / size_of::<K>() + size_of::<V>();
        for pair in &self.pairs {
            if pos > size {
                break;
            }

            if let Some(pair) = pair {
                let key: BytesMut = pair.a.into();
                let value: BytesMut = pair.b.into();

                put_bytes!(page.data, key, pos, key.len());
                pos += key.len();
                put_bytes!(page.data, value, pos, value.len());
                pos += value.len();
            } else {
                put_bytes!(page.data, &BytesMut::zeroed(size), pos, size);
                pos += size;
            }
        }
    }

    pub fn get_pairs(&self) -> Vec<Pair<K, V>> {
        let mut ret = Vec::new();
        for pair in &self.pairs {
            if let Some(pair) = pair {
                ret.push(*pair);
            }
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
        page::{SharedPage, DEFAULT_PAGE_SIZE},
    };

    #[tokio::test]
    async fn test_bucket() {
        let page = SharedPage::<DEFAULT_PAGE_SIZE>::new(0);
        let mut page_w = page.write().await;

        let mut bucket: Bucket<i32, i32, DEFAULT_PAGE_SIZE, DEFAULT_BIT_SIZE> =
            Bucket::new(&page_w.data);

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
        let bucket: Bucket<i32, i32, DEFAULT_PAGE_SIZE, DEFAULT_BIT_SIZE> =
            Bucket::new(&page_w.data);
        assert!(bucket.get_at(0).unwrap() == (1, 2));
        assert!(bucket.get_at(1).unwrap() == (3, 4));
        assert!(bucket.get_at(2).unwrap() == (5, 6));
        assert!(bucket.get_at(3).is_none());

        let find1 = bucket.find(&1);
        assert!(find1.len() == 1);
        assert!(find1[0] == 2);
    }
}
