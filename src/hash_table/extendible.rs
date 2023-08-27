use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use bytes::BytesMut;

use crate::{
    hash_table::bucket_page::Bucket,
    hash_table::dir_page::{self, Directory},
    page::PageID,
    page_manager::BufferPool,
    pair::PairType,
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
    pub fn new(dir_page_id: PageID, bpm: BufferPool<SIZE, PAGE_SIZE>) -> Self {
        Self {
            dir_page_id,
            bpm,
            _data: PhantomData,
        }
    }

    pub async fn insert(&self, k: &K, v: &V) -> bool {
        let dir_page = match self.bpm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => panic!("could not fetch directory page"),
        };
        let mut dir_page_w = dir_page.write().await;
        let mut dir = Directory::new(&dir_page_w.data);

        let i = Self::key_to_directory_index(k, &dir);
        let bucket_page_id = dir.get_page_id(i);
        let bucket_page = if bucket_page_id == 0 {
            match self.bpm.new_page().await {
                Some(p) => {
                    dir.set_page_id(i, p.read().await.id);
                    dir.write_data(&mut dir_page_w);
                    p
                }
                None => panic!("could not create bucket page"),
            }
        } else {
            match self.bpm.fetch_page(bucket_page_id).await {
                Some(p) => p,
                None => panic!("count not fetch bucket page"),
            }
        };

        let mut bucket_page_w = bucket_page.write().await;
        let mut bucket: Bucket<K, V, PAGE_SIZE> = Bucket::new(&bucket_page_w.data);

        bucket.insert(k, v);
        bucket.write_data(&mut bucket_page_w);

        drop(dir_page_w);
        drop(bucket_page_w);
        self.bpm.unpin_page(dir_page.get_id()).await;
        self.bpm.unpin_page(bucket_page.get_id()).await;

        true
    }

    pub async fn remove(&self, k: &K, v: &V) -> bool {
        let dir_page = match self.bpm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => panic!("could not fetch directory page"),
        };
        let dir_page_r = dir_page.read().await;
        let dir = Directory::new(&dir_page_r.data);

        let i = Self::key_to_directory_index(k, &dir);
        let bucket_page_id = dir.get_page_id(i);
        let bucket_page = if bucket_page_id == 0 {
            return false;
        } else {
            match self.bpm.fetch_page(bucket_page_id).await {
                Some(p) => p,
                None => panic!("count not fetch bucket page"),
            }
        };
        let mut bucket_page_w = bucket_page.write().await;
        let mut bucket: Bucket<K, V, PAGE_SIZE> = Bucket::new(&bucket_page_w.data);

        let ret = bucket.remove(k, v);
        bucket.write_data(&mut bucket_page_w);

        drop(dir_page_r);
        drop(bucket_page_w);
        self.bpm.unpin_page(dir_page.get_id()).await;
        self.bpm.unpin_page(bucket_page.get_id()).await;

        ret
    }

    pub async fn get(&self, k: &K) -> Vec<V> {
        let dir_page = match self.bpm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => panic!("could not fetch directory page"),
        };
        let dir_page_r = dir_page.read().await;
        let dir = Directory::new(&dir_page_r.data);

        let i = Self::key_to_directory_index(k, &dir);
        let bucket_page_id = dir.get_page_id(i);
        let bucket_page = if bucket_page_id == 0 {
            return vec![];
        } else {
            match self.bpm.fetch_page(bucket_page_id).await {
                Some(p) => p,
                None => panic!("count not fetch bucket page"),
            }
        };

        let bucket_page_w = bucket_page.read().await;
        let bucket: Bucket<K, V, PAGE_SIZE> = Bucket::new(&bucket_page_w.data);

        drop(dir_page_r);
        drop(bucket_page_w);
        self.bpm.unpin_page(dir_page.get_id()).await;
        self.bpm.unpin_page(bucket_page.get_id()).await;

        bucket.find(k)
    }

    fn hash(k: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn key_to_directory_index(k: &K, dir_page: &Directory<PAGE_SIZE>) -> usize {
        let hash = Self::hash(k);
        let i = hash & dir_page.get_global_depth_mask();

        i % dir_page::BUCKET_PAGE_IDS_SIZE_U32
    }
}

#[cfg(test)]
mod test {
    use crate::{
        disk::Disk, hash_table::extendible::ExtendibleHashTable, page::DEFAULT_PAGE_SIZE,
        page_manager::BufferPool, replacer::LrukReplacer, test::CleanUp,
    };

    #[tokio::test]
    async fn test_extendible_hash_table() {
        let file = "test_extendible_hash_table.db";
        let disk = Disk::<DEFAULT_PAGE_SIZE>::new(file)
            .await
            .expect("could not open db file");
        let _cu = CleanUp::file(file);
        let replacer = LrukReplacer::new(2);
        let bpm = BufferPool::<8, DEFAULT_PAGE_SIZE>::new(disk, replacer, 0);
        let _dir_page = bpm.new_page().await;
        let ht = ExtendibleHashTable::new(0, bpm.clone());

        ht.insert(&0, &1).await;
        ht.insert(&2, &3).await;
        ht.insert(&4, &5).await;

        let r1 = ht.get(&0).await;
        let r2 = ht.get(&2).await;
        let r3 = ht.get(&4).await;

        assert!(r1[0] == 1);
        assert!(r2[0] == 3);
        assert!(r3[0] == 5);

        ht.remove(&4, &5).await;

        bpm.flush_all_pages().await;

        // Make sure it reads back ok
        let disk = Disk::<DEFAULT_PAGE_SIZE>::new(file)
            .await
            .expect("could not open db file");
        let replacer = LrukReplacer::new(2);
        let bpm = BufferPool::<8, DEFAULT_PAGE_SIZE>::new(disk, replacer, 0);
        let ht: ExtendibleHashTable<8, DEFAULT_PAGE_SIZE, i32, i32> =
            ExtendibleHashTable::new(0, bpm.clone());

        let r1 = ht.get(&0).await;
        let r2 = ht.get(&2).await;
        let r3 = ht.get(&4).await;

        assert!(r1[0] == 1);
        assert!(r2[0] == 3);
        assert!(r3.is_empty());
    }
}
