// No hashing in the bucket, key/value pairs are inserted/fetched by scanning

use std::mem::size_of;

use bytes::BytesMut;
use tokio::sync::RwLockWriteGuard;

use crate::{
    copy_bytes, get_bytes,
    page::{Page, DEFAULT_PAGE_SIZE},
    put_bytes,
};

pub const OCCUPIED_SIZE: usize = 512;
pub const READABLE_SIZE: usize = 512;
pub const VALUES_START: usize = OCCUPIED_SIZE + READABLE_SIZE;

pub struct Bucket<K, V, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    occupied: [u8; OCCUPIED_SIZE],
    readable: [u8; READABLE_SIZE],
    pairs: Vec<(PairType<K>, PairType<V>)>,
}

impl<'a, const PAGE_SIZE: usize, K, V> Bucket<K, V, PAGE_SIZE>
where
    PairType<K>: From<BytesMut> + Into<BytesMut> + PartialEq + Copy,
    PairType<V>: From<BytesMut> + Into<BytesMut> + PartialEq + Copy,
{
    pub fn write(page: &RwLockWriteGuard<'_, Page<PAGE_SIZE>>) -> Self {
        let data = &page.data;

        let mut occupied = [0; OCCUPIED_SIZE];
        copy_bytes!(occupied, data, 0, OCCUPIED_SIZE);

        let mut readable = [0; READABLE_SIZE];
        copy_bytes!(readable, data, OCCUPIED_SIZE, READABLE_SIZE);

        let k_size = size_of::<K>();
        let v_size = size_of::<V>();

        let mut pairs = Vec::with_capacity(PAGE_SIZE / k_size + v_size);
        let mut pos = VALUES_START;
        while pos < PAGE_SIZE {
            let k_bytes = BytesMut::from(get_bytes!(data, pos, k_size));
            let v_bytes = BytesMut::from(get_bytes!(data, pos, v_size));

            let k: PairType<K> = k_bytes.into();
            let v: PairType<V> = v_bytes.into();
            pairs.push((k, v));

            pos += k_size + v_size;
        }

        Self {
            occupied,
            readable,
            pairs,
        }
    }

    pub fn set_occupied(&mut self, i: usize, val: bool) {
        let pos_i = i / 8;
        let pos_j = i % 8;

        let b = &mut self.occupied[pos_i];

        if val {
            *b |= 1 << pos_j;
        } else {
            *b &= !(1 << pos_j);
        }
    }

    pub fn is_occupied(&self, i: usize) -> bool {
        let pos_i = i / 8;
        let pos_j = i % 8;

        let b = self.occupied[pos_i];

        if b & (1 << pos_j) == 1 {
            true
        } else {
            false
        }
    }

    pub fn set_readable(&mut self, i: usize, val: bool) {
        let pos_i = i / 8;
        let pos_j = i % 8;

        let b = &mut self.readable[pos_i];

        if val {
            *b |= 1 << pos_j;
        } else {
            *b &= !(1 << pos_j);
        }
    }

    pub fn is_readable(&self, i: usize) -> bool {
        let pos_i = i / 8;
        let pos_j = i % 8;

        let b = self.readable[pos_i];

        if b & (1 << pos_j) == 1 {
            true
        } else {
            false
        }
    }

    pub fn remove(&mut self, k: PairType<K>, v: PairType<V>) {
        let mut delete = Vec::new();
        for (i, (k_, v_)) in self.pairs.iter().enumerate() {
            if k == *k_ && v == *v_ {
                delete.push(i);
            }
        }

        for i in delete {
            self.set_readable(i, false);
            self.set_occupied(i, false);
        }
    }

    pub fn insert(&mut self, k: PairType<K>, v: PairType<V>) {
        // Find occupied
        let mut i = 0;
        loop {
            if !self.is_occupied(i) {
                break;
            }

            i += 1;
        }

        self.pairs[i] = (k, v);
        self.set_occupied(i, true);
        self.set_readable(i, true);
    }

    pub fn get(&self, i: usize) -> &(PairType<K>, PairType<V>) {
        &self.pairs[i]
    }

    pub fn as_bytes(&self) -> BytesMut {
        let mut ret = BytesMut::zeroed(PAGE_SIZE);

        put_bytes!(ret, self.occupied, 0, OCCUPIED_SIZE);
        put_bytes!(ret, self.readable, OCCUPIED_SIZE, READABLE_SIZE);

        let mut pos = OCCUPIED_SIZE + READABLE_SIZE;
        for pair in &self.pairs {
            let key: BytesMut = pair.0.into();
            let value: BytesMut = pair.1.into();

            put_bytes!(ret, key, pos, key.len());
            pos += key.len();
            put_bytes!(ret, value, pos, value.len());
            pos += value.len();
        }

        ret
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct PairType<T>(T);

impl Into<BytesMut> for PairType<i32> {
    fn into(self) -> BytesMut {
        let mut ret = BytesMut::zeroed(size_of::<i32>());
        copy_bytes!(ret, i32::to_be_bytes(self.0), 0, size_of::<i32>());

        ret
    }
}

impl From<BytesMut> for PairType<i32> {
    fn from(value: BytesMut) -> Self {
        let mut bytes = [0; size_of::<i32>()];
        copy_bytes!(bytes, value[0..size_of::<i32>()], 0, size_of::<i32>());

        PairType(i32::from_be_bytes(bytes))
    }
}

impl Into<PairType<i32>> for i32 {
    fn into(self) -> PairType<i32> {
        PairType(self)
    }
}

impl PartialEq<PairType<i32>> for i32 {
    fn eq(&self, other: &PairType<i32>) -> bool {
        *self == other.0
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table_bucket_page::{Bucket, PairType},
        page::{SharedPage, DEFAULT_PAGE_SIZE},
    };

    #[tokio::test]
    async fn test_directory() {
        let page = SharedPage::<DEFAULT_PAGE_SIZE>::new(0);
        let mut page_w = page.write().await;

        let mut bucket = Bucket::write(&page_w);

        bucket.insert(1.into(), 2.into());
        bucket.insert(3.into(), 4.into());
        bucket.insert(5.into(), 6.into());

        let got = bucket.get(0);
        assert!(bucket.get(0) == &(PairType(1), PairType(2)), "Got: {got:?}");
        // assert!(bucket.get(3) == &(PairType(3), PairType(4)));
        // assert!(bucket.get(5) == &(PairType(5), PairType(6)));

        let bucket_bytes = bucket.as_bytes();
        assert!(bucket_bytes.len() == DEFAULT_PAGE_SIZE);
        page_w.data = bucket_bytes;

        drop(bucket);

        // Make sure it reads back ok
        let bucket = Bucket::write(&page_w);
        let got = bucket.get(0);
        assert!(got == &(PairType(1), PairType(2)), "Got: {got:?}");
        // assert!(dir.get(3) == &(PairType(3), PairType(4)));
        // assert!(dir.get(5) == &(PairType(5), PairType(6)));
    }
}
