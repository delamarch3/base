use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use crate::{
    hash_table::bucket_page::{Bucket, DEFAULT_BIT_SIZE},
    hash_table::dir_page::{self, Directory},
    page::PageId,
    page_manager::PageCache,
    storable::Storable,
};

pub struct ExtendibleHashTable<
    K,
    V,
    const POOL_SIZE: usize,
    const BUCKET_BIT_SIZE: usize = DEFAULT_BIT_SIZE,
> {
    dir_page_id: PageId,
    pm: PageCache<POOL_SIZE>,
    _data: PhantomData<(K, V)>,
}

impl<const POOL_SIZE: usize, const BUCKET_BIT_SIZE: usize, K, V>
    ExtendibleHashTable<K, V, POOL_SIZE, BUCKET_BIT_SIZE>
where
    K: Storable + Copy + Eq + Hash,
    V: Storable + Copy + Eq,
{
    pub fn new(dir_page_id: PageId, pm: PageCache<POOL_SIZE>) -> Self {
        Self {
            dir_page_id,
            pm,
            _data: PhantomData,
        }
    }

    pub async fn insert(&self, k: &K, v: &V) -> bool {
        let dir_page = match self.pm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch directory page"),
        };
        let mut dir_page_w = dir_page.write().await;
        let mut dir = Directory::new(&dir_page_w.data);

        let bucket_index = Self::get_bucket_index(k, &dir);
        let bucket_page_id = dir.get_page_id(bucket_index);
        let bucket_page = if bucket_page_id == 0 {
            match self.pm.new_page().await {
                Some(p) => {
                    dir.set_bucket_page_id(bucket_index, p.read().await.id);
                    dir.write_data(&mut dir_page_w);
                    p
                }
                None => unimplemented!("could not create bucket page"),
            }
        } else {
            match self.pm.fetch_page(bucket_page_id).await {
                Some(p) => p,
                None => unimplemented!("cound not fetch bucket page"),
            }
        };

        let mut bucket_page_w = bucket_page.write().await;
        let mut bucket: Bucket<K, V, BUCKET_BIT_SIZE> = Bucket::new(&bucket_page_w.data);

        bucket.insert(k, v);
        bucket.write_data(&mut bucket_page_w);

        if bucket.is_full() {
            if dir.get_local_depth_mask(bucket_index) == dir.get_global_depth_mask() {
                // The size of the directory implicitily doubles
                dir.incr_global_depth();
            }

            // 1. Create two new bucket pages and increase local depths for both of them
            // 2. Get the high bit of the old bucket (1 << local_depth)
            // 3. Reinsert into the new pages
            // 4. Update the page ids in the directory
            let page0 = match self.pm.new_page().await {
                Some(p) => p,
                None => unimplemented!("could not create a new page for bucket split"),
            };
            let page0_w = page0.write().await;
            let mut bucket0: Bucket<K, V, BUCKET_BIT_SIZE> = Bucket::new(&page0_w.data);

            let page1 = match self.pm.new_page().await {
                Some(p) => p,
                None => unimplemented!("could not create a new page for bucket split"),
            };
            let page1_w = page1.write().await;
            let mut bucket1: Bucket<K, V, BUCKET_BIT_SIZE> = Bucket::new(&page1_w.data);

            let bit = dir.get_local_high_bit(bucket_index);
            for pair in bucket.get_pairs() {
                let i = Self::get_bucket_index(&pair.a, &dir);
                let new_bucket = if i & bit > 0 {
                    &mut bucket1
                } else {
                    &mut bucket0
                };
                new_bucket.insert(&pair.a, &pair.b);
            }

            for i in (Self::get_bucket_index(&k, &dir) & (bit - 1)
                ..dir_page::DEFAULT_BUCKET_PAGE_IDS_SIZE)
                .step_by(bit)
            {
                let new_page_id = if i & bit > 0 { page0_w.id } else { page1_w.id };

                dir.set_bucket_page_id(i, new_page_id);
            }

            dir.write_data(&mut dir_page_w);

            // TODO: mark original page on disk as ready to be allocated
        }

        let dir_page_id = dir_page_w.id;
        let bucket_page_id = bucket_page_w.id;
        drop(dir_page_w);
        drop(bucket_page_w);
        self.pm.unpin_page(dir_page_id).await;
        self.pm.unpin_page(bucket_page_id).await;

        true
    }

    pub async fn remove(&self, k: &K, v: &V) -> bool {
        let dir_page = match self.pm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch directory page"),
        };
        let dir_page_r = dir_page.read().await;
        let dir = Directory::new(&dir_page_r.data);

        let bucket_index = Self::get_bucket_index(k, &dir);
        let bucket_page_id = dir.get_page_id(bucket_index);
        let bucket_page = if bucket_page_id == 0 {
            return false;
        } else {
            match self.pm.fetch_page(bucket_page_id).await {
                Some(p) => p,
                None => unimplemented!("cound not fetch bucket page"),
            }
        };
        let mut bucket_page_w = bucket_page.write().await;
        let mut bucket: Bucket<K, V, BUCKET_BIT_SIZE> = Bucket::new(&bucket_page_w.data);

        let ret = bucket.remove(k, v);
        bucket.write_data(&mut bucket_page_w);

        // TODO: attempt to merge if empty

        let dir_page_id = dir_page_r.id;
        let bucket_page_id = bucket_page_w.id;
        drop(dir_page_r);
        drop(bucket_page_w);
        self.pm.unpin_page(dir_page_id).await;
        self.pm.unpin_page(bucket_page_id).await;

        ret
    }

    pub async fn get(&self, k: &K) -> Vec<V> {
        let dir_page = match self.pm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch directory page"),
        };
        let dir_page_r = dir_page.read().await;
        let dir = Directory::new(&dir_page_r.data);

        let bucket_index = Self::get_bucket_index(k, &dir);
        let bucket_page_id = dir.get_page_id(bucket_index);
        let bucket_page = if bucket_page_id == 0 {
            return vec![];
        } else {
            match self.pm.fetch_page(bucket_page_id).await {
                Some(p) => p,
                None => unimplemented!("cound not fetch bucket page"),
            }
        };

        let bucket_page_w = bucket_page.read().await;
        let bucket: Bucket<K, V, BUCKET_BIT_SIZE> = Bucket::new(&bucket_page_w.data);

        let dir_page_id = dir_page_r.id;
        let bucket_page_id = bucket_page_w.id;
        drop(dir_page_r);
        drop(bucket_page_w);
        self.pm.unpin_page(dir_page_id).await;
        self.pm.unpin_page(bucket_page_id).await;

        bucket.find(k)
    }

    pub async fn get_num_buckets(&self) -> u32 {
        let dir_page = match self.pm.fetch_page(self.dir_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch directory page"),
        };
        let dir_page_r = dir_page.read().await;
        let dir = Directory::new(&dir_page_r.data);

        1 << dir.get_global_depth()
    }

    fn hash(k: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn get_bucket_index(k: &K, dir_page: &Directory) -> usize {
        let hash = Self::hash(k);
        let i = hash & dir_page.get_global_depth_mask();

        i % dir_page::DEFAULT_BUCKET_PAGE_IDS_SIZE
    }
}

#[cfg(test)]
mod test {
    use crate::{
        disk::Disk,
        hash_table::extendible::ExtendibleHashTable,
        hash_table::{bucket_page::DEFAULT_BIT_SIZE, dir_page::Directory},
        page_manager::PageCache,
        replacer::LRUKReplacer,
        test::CleanUp,
    };

    #[tokio::test]
    async fn test_extendible_hash_table() {
        let file = "test_extendible_hash_table.db";
        let disk = Disk::new(file).await.expect("could not open db file");
        let _cu = CleanUp::file(file);
        let replacer = LRUKReplacer::new(2);
        let dir_page_id = 0;
        const POOL_SIZE: usize = 8;
        let pm = PageCache::<POOL_SIZE>::new(disk, replacer, dir_page_id);
        let _dir_page = pm.new_page().await;
        let ht: ExtendibleHashTable<i32, i32, POOL_SIZE, DEFAULT_BIT_SIZE> =
            ExtendibleHashTable::new(dir_page_id, pm.clone());

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

        pm.flush_all_pages().await;

        // Make sure it reads back ok
        let disk = Disk::new(file).await.expect("could not open db file");
        let replacer = LRUKReplacer::new(2);
        let pm = PageCache::<8>::new(disk, replacer, dir_page_id + 1);
        let ht: ExtendibleHashTable<i32, i32, 8, DEFAULT_BIT_SIZE> =
            ExtendibleHashTable::new(dir_page_id, pm.clone());

        let r1 = ht.get(&0).await;
        let r2 = ht.get(&2).await;
        let r3 = ht.get(&4).await;

        assert!(r1[0] == 1);
        assert!(r2[0] == 3);
        assert!(r3.is_empty());
    }

    #[tokio::test]
    async fn test_split() {
        let file = "test_split.db";
        let disk = Disk::new(file).await.expect("could not open db file");
        let _cu = CleanUp::file(file);
        let replacer = LRUKReplacer::new(2);
        let dir_page_id = 0;
        const POOL_SIZE: usize = 8;
        const BIT_SIZE: usize = 1; // 8 slots
        let pm = PageCache::<POOL_SIZE>::new(disk, replacer, dir_page_id);
        let _dir_page = pm.new_page().await;
        let ht: ExtendibleHashTable<i32, i32, POOL_SIZE, BIT_SIZE> =
            ExtendibleHashTable::new(dir_page_id, pm.clone());

        assert!(ht.get_num_buckets().await == 1);

        // Global depth should be 1 after this
        ht.insert(&0, &1).await;
        ht.insert(&2, &2).await;
        ht.insert(&0, &3).await;
        ht.insert(&2, &4).await;
        ht.insert(&0, &5).await;
        ht.insert(&2, &6).await;
        ht.insert(&0, &7).await;
        ht.insert(&2, &8).await;

        assert!(ht.get_num_buckets().await == 2);

        let dir_page = pm.fetch_page(0).await.expect("there should be a page 0");
        let dir_page_w = dir_page.write().await;
        let dir = Directory::new(&dir_page_w.data);

        assert!(dir.get_global_depth() == 1);
    }
}
