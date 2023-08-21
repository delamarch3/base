// No hashing in the bucket, key/value pairs are inserted/fetched by scanning

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

pub const OCCUPIED_SIZE: usize = 512 / 8;
pub const READABLE_SIZE: usize = 512 / 8;
pub const VALUES_START: usize = OCCUPIED_SIZE + READABLE_SIZE;

pub struct Bucket<K, V, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    pub occupied: BitMap<OCCUPIED_SIZE>,
    pub readable: BitMap<READABLE_SIZE>,
    pairs: Vec<Pair<K, V>>,
}

impl<'a, const PAGE_SIZE: usize, K, V> Bucket<K, V, PAGE_SIZE>
where
    PairType<K>: Into<BytesMut> + From<&'a [u8]> + PartialEq<K> + Copy,
    PairType<V>: Into<BytesMut> + From<&'a [u8]> + PartialEq<V> + Copy,
    V: Copy,
{
    pub fn write(page: &'a RwLockWriteGuard<'_, Page<PAGE_SIZE>>) -> Self {
        let data = &page.data;

        let mut occupied = BitMap::<OCCUPIED_SIZE>::new();
        copy_bytes!(occupied.as_mut_slice(), data, 0, OCCUPIED_SIZE);

        let mut readable = BitMap::<READABLE_SIZE>::new();
        copy_bytes!(readable.as_mut_slice(), data, OCCUPIED_SIZE, READABLE_SIZE);

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();

        let mut pairs = Vec::with_capacity(PAGE_SIZE / k_size + v_size);
        let mut pos = VALUES_START;
        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_size;
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_size;

            pairs.push(Pair::from_bytes(k_bytes, v_bytes));
        }

        Self {
            occupied,
            readable,
            pairs,
        }
    }

    pub fn remove(&mut self, k: K, v: V) {
        let mut delete = Vec::new();
        for (i, pair) in self.pairs.iter().enumerate() {
            if pair.a == k && pair.b == v {
                delete.push(i);
            }
        }

        for i in delete {
            self.readable.set(i, false);
            self.occupied.set(i, false);
        }
    }

    pub fn insert(&mut self, k: K, v: V) {
        // Find occupied
        let mut i = 0;
        loop {
            if !self.occupied.check(i) {
                break;
            }

            i += 1;
        }

        self.pairs[i] = Pair::new(k, v);
        self.occupied.set(i, true);
        self.readable.set(i, true);
    }

    pub fn get(&self, i: usize) -> Option<&Pair<K, V>> {
        if self.readable.check(i) {
            Some(&self.pairs[i])
        } else {
            None
        }
    }

    pub fn find(&self, k: &K) -> Vec<V> {
        let mut ret = Vec::new();
        for (i, pair) in self.pairs.iter().enumerate() {
            if pair.a == *k && self.readable.check(i) {
                ret.push(pair.b.0)
            }
        }

        ret
    }

    pub fn as_bytes(&self) -> BytesMut {
        let mut ret = BytesMut::zeroed(PAGE_SIZE);

        put_bytes!(ret, self.occupied.as_slice(), 0, OCCUPIED_SIZE);
        put_bytes!(ret, self.readable.as_slice(), OCCUPIED_SIZE, READABLE_SIZE);

        let mut pos = OCCUPIED_SIZE + READABLE_SIZE;
        for pair in &self.pairs {
            let key: BytesMut = pair.a.into();
            let value: BytesMut = pair.b.into();

            put_bytes!(ret, key, pos, key.len());
            pos += key.len();
            put_bytes!(ret, value, pos, value.len());
            pos += value.len();
        }

        ret
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table_bucket_page::Bucket,
        page::{SharedPage, DEFAULT_PAGE_SIZE},
    };

    #[tokio::test]
    async fn test_bucket() {
        let page = SharedPage::<DEFAULT_PAGE_SIZE>::new(0);
        let mut page_w = page.write().await;

        let mut bucket = Bucket::write(&page_w);

        bucket.insert(1, 2);
        bucket.insert(3, 4);
        bucket.insert(5, 6);
        bucket.insert(7, 8);
        bucket.remove(7, 8);

        assert!(*bucket.get(0).unwrap() == (1, 2));
        assert!(*bucket.get(1).unwrap() == (3, 4));
        assert!(*bucket.get(2).unwrap() == (5, 6));
        assert!(bucket.get(3).is_none());

        let bucket_bytes = bucket.as_bytes();
        assert!(bucket_bytes.len() == DEFAULT_PAGE_SIZE);
        page_w.data = bucket_bytes;

        drop(bucket);

        // Make sure it reads back ok
        let bucket = Bucket::write(&page_w);
        assert!(*bucket.get(0).unwrap() == (1, 2));
        assert!(*bucket.get(1).unwrap() == (3, 4));
        assert!(*bucket.get(2).unwrap() == (5, 6));
        assert!(bucket.get(3).is_none());

        let find1 = bucket.find(&1);
        assert!(find1.len() == 1);
        assert!(find1[0] == 2);
    }
}
