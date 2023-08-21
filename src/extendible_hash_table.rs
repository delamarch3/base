use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use bytes::BytesMut;

use crate::{
    hash_table_bucket_page::Bucket, hash_table_page::Directory, page::PageID,
    page_manager::BufferPool, pair::PairType,
};

pub struct ExtendibleHashTable<const SIZE: usize, const PAGE_SIZE: usize, K, V> {
    dir_page_id: PageID,
    bpm: BufferPool<SIZE, PAGE_SIZE>,
    _data: PhantomData<(K, V)>,
}

impl<const SIZE: usize, const PAGE_SIZE: usize, K, V> ExtendibleHashTable<SIZE, PAGE_SIZE, K, V>
where
    for<'a> PairType<K>: Into<BytesMut> + From<&'a [u8]> + PartialEq<K> + Copy,
    for<'a> PairType<V>: Into<BytesMut> + From<&'a [u8]> + PartialEq<V> + Copy,
    K: Copy + Hash,
    V: Copy,
{
    pub async fn insert(&self, k: &K, v: &V) {
        let i = Self::hash(k);

        let page = match self.bpm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => panic!("could not fetch directory page"),
        };
        let page_w = page.write().await;
        let dir = Directory::write(&page_w);

        let bucket_page_id = dir.get_page_id(i);

        let bucket_page = match self.bpm.fetch_page(bucket_page_id).await {
            Some(p) => p,
            None => panic!("count not fetch bucket page"),
        };
        let mut bucket_page_w = bucket_page.write().await;
        let mut bucket: Bucket<K, V, PAGE_SIZE> = Bucket::write(&bucket_page_w);

        bucket.insert(k, v);
        bucket_page_w.data = bucket.as_bytes();
    }

    fn hash(k: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    }
}
