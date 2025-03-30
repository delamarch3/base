use std::mem::size_of;
use std::ops::Range;

use crate::bitmap::BitMap;
use crate::catalog::schema::Schema;
use crate::page::{PageBuf, PAGE_SIZE};
use crate::pair::Pair;
use crate::storable::Storable;
use crate::table::tuple::Data as TupleData;

/// Number of bytes for the bitmaps
pub const BITMAP_SIZE: usize = 512 / 8;

const OCCUPIED: Range<usize> = 0..BITMAP_SIZE;
const READABLE: Range<usize> = BITMAP_SIZE..BITMAP_SIZE + BITMAP_SIZE;

pub struct Bucket<V> {
    pub occupied: BitMap<BITMAP_SIZE>,
    pub readable: BitMap<BITMAP_SIZE>,
    pairs: [Option<Pair<TupleData, V>>; 512],
    key_size: usize,
}

impl<V> From<&Bucket<V>> for PageBuf
where
    V: Storable,
{
    fn from(bucket: &Bucket<V>) -> Self {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[OCCUPIED].copy_from_slice(bucket.occupied.as_slice());
        ret[READABLE].copy_from_slice(bucket.occupied.as_slice());

        let mut pos = BITMAP_SIZE * 2;
        let key_size = bucket.key_size;
        let pair_size = key_size + size_of::<V>();
        for pair in &bucket.pairs {
            if pos + pair_size > PAGE_SIZE {
                break;
            }

            if let Some(pair) = pair {
                ret[pos..pos + key_size].copy_from_slice(pair.a.as_bytes());
                pos += pair.a.size();
                pair.b.write_to(&mut ret, pos);
                pos += pair.b.size();
            }
        }

        ret
    }
}

impl<V> From<Bucket<V>> for PageBuf
where
    V: Storable,
{
    fn from(bucket: Bucket<V>) -> Self {
        Self::from(&bucket)
    }
}

impl<V> Bucket<V>
where
    V: Storable + Copy + Eq,
{
    pub fn deserialise_page(buf: &PageBuf, schema: &Schema) -> Self {
        let mut occupied = BitMap::<BITMAP_SIZE>::new();
        occupied.as_mut_slice().copy_from_slice(&buf[OCCUPIED]);

        let mut readable = BitMap::<BITMAP_SIZE>::new();
        readable.as_mut_slice().copy_from_slice(&buf[READABLE]);

        // Use the occupied map to find pairs to insert
        let mut pairs: [Option<Pair<TupleData, V>>; 512] = std::array::from_fn(|_| None);

        let key_size = schema.tuple_size();
        let value_size = size_of::<V>();

        let mut pos = BITMAP_SIZE * 2;
        for (i, pair) in pairs.iter_mut().enumerate() {
            if !occupied.check(i) {
                pos += key_size + value_size;
                continue;
            }

            let key = TupleData::new(&buf[pos..pos + key_size]);
            pos += key_size;

            let value = V::from_bytes(&buf[pos..pos + value_size]);
            pos += value_size;

            *pair = Some(Pair::new(key, value));
        }

        Self { occupied, readable, pairs, key_size }
    }

    pub fn remove(&mut self, key: &TupleData, value: &V) -> bool {
        let mut ret = false;
        for (i, pair) in self.pairs.iter().enumerate() {
            if let Some(pair) = pair {
                if pair.a == *key && pair.b == *value {
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

    pub fn insert(&mut self, key: TupleData, value: V) {
        let mut i = 0;
        while self.occupied.check(i) {
            i += 1
        }

        if i >= self.pairs.len() {
            // TODO
        }

        self.pairs[i] = Some(Pair::new(key, value));
        self.occupied.set(i, true);
        self.readable.set(i, true);
    }

    pub fn get(&self, i: usize) -> Option<&Pair<TupleData, V>> {
        if self.readable.check(i) {
            self.pairs[i].as_ref()
        } else {
            None
        }
    }

    pub fn find(&self, key: &TupleData) -> Vec<V> {
        let mut ret = Vec::new();
        for (i, pair) in self.pairs.iter().enumerate() {
            if let Some(pair) = pair {
                if pair.a == *key && self.readable.check(i) {
                    ret.push(pair.b)
                }
            }
        }

        ret
    }

    pub fn get_pairs(self) -> Vec<Pair<TupleData, V>> {
        // TODO: this should only return the readable pairs
        self.pairs.into_iter().flatten().collect()
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        let len = self.occupied.len();
        let s = self.key_size + size_of::<V>();

        len >= (PAGE_SIZE - 128) / s
    }
}

#[cfg(test)]
mod test {
    use crate::hash_table::bucket::Bucket;
    use crate::page::Page;
    use crate::table::tuple::Builder as TupleBuilder;
    use crate::{column, schema};

    #[test]
    fn test_bucket() {
        let page = Page::default();
        let mut page_w = page.write();
        let key_schema = schema! { column!("c1", Int) };

        let mut bucket: Bucket<i32> = Bucket::deserialise_page(&page_w.data, &key_schema);

        let keys = [1, 3, 5, 7, 9].map(|n| TupleBuilder::new().int(n).build());
        let values = [2, 4, 6, 8, 10];

        for (key, value) in std::iter::zip(keys.clone(), values.clone()) {
            bucket.insert(key, value);
        }

        assert_eq!(bucket.get(0).unwrap(), &(keys[0].clone(), values[0]));
        assert_eq!(bucket.get(1).unwrap(), &(keys[1].clone(), values[1]));
        assert_eq!(bucket.get(2).unwrap(), &(keys[2].clone(), values[2]));
        assert_eq!(bucket.get(3).unwrap(), &(keys[3].clone(), values[3]));
        assert_eq!(bucket.get(4).unwrap(), &(keys[4].clone(), values[4]));
        assert!(bucket.get(5).is_none());

        page_w.put_object(bucket);

        // Make sure it reads back ok
        let mut bucket: Bucket<i32> = Bucket::deserialise_page(&page_w.data, &key_schema);
        assert_eq!(bucket.get(0).unwrap(), &(keys[0].clone(), values[0]));
        assert_eq!(bucket.get(1).unwrap(), &(keys[1].clone(), values[1]));
        assert_eq!(bucket.get(2).unwrap(), &(keys[2].clone(), values[2]));
        assert_eq!(bucket.get(3).unwrap(), &(keys[3].clone(), values[3]));
        assert_eq!(bucket.get(4).unwrap(), &(keys[4].clone(), values[4]));
        assert!(bucket.get(5).is_none());

        bucket.insert(keys[0].clone(), 11);
        let found = bucket.find(&keys[0]);
        assert_eq!(found, [2, 11]);
    }
}
