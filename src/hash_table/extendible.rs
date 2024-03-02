use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use crate::{
    disk::{Disk, FileSystem},
    hash_table::bucket_page::Bucket,
    hash_table::dir_page::{self, Directory},
    page::{PageBuf, PageId},
    page_cache::SharedPageCache,
    storable::Storable,
    writep,
};

pub struct ExtendibleHashTable<K, V, D: Disk = FileSystem> {
    dir_page_id: PageId,
    pc: SharedPageCache<D>,
    _data: PhantomData<(K, V)>,
}

impl<K, V, D> ExtendibleHashTable<K, V, D>
where
    K: Storable + Copy + Eq + Hash,
    V: Storable + Copy + Eq,
    D: Disk,
{
    pub fn new(dir_page_id: PageId, pc: SharedPageCache<D>) -> Self {
        Self {
            dir_page_id,
            pc,
            _data: PhantomData,
        }
    }

    pub fn insert(&self, k: &K, v: &V) -> crate::Result<bool> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let mut dir_page_w = dir_page.page.write();
        let mut dir = Directory::from(&dir_page_w.data);

        let bucket_index = Self::get_bucket_index(k, &dir);
        let bucket_page_id = dir.get(bucket_index);
        let bucket_page = match bucket_page_id {
            0 => {
                let p = self.pc.new_page()?;
                dir.insert(bucket_index, p.page.read().id);
                writep!(dir_page_w, &PageBuf::from(&dir));
                p
            }
            _ => self.pc.fetch_page(bucket_page_id)?,
        };

        let mut bucket_page_w = bucket_page.page.write();
        let mut bucket = Bucket::from(&bucket_page_w.data);

        bucket.insert(k, v);
        writep!(bucket_page_w, &PageBuf::from(&bucket));

        if bucket.is_full() {
            if dir.local_depth_mask(bucket_index) == dir.global_depth_mask() {
                // The size of the directory implicitily doubles
                dir.incr_global_depth();
            }

            // 1. Create two new bucket pages and increase local depths for both of them
            // 2. Get the high bit of the old bucket (1 << local_depth)
            // 3. Reinsert into the new pages
            // 4. Update the page ids in the directory
            let page0 = self.pc.new_page()?;
            let mut page0_w = page0.page.write();
            let mut bucket0 = Bucket::from(&page0_w.data);

            let page1 = self.pc.new_page()?;
            let mut page1_w = page1.page.write();
            let mut bucket1 = Bucket::from(&page1_w.data);

            let bit = dir.get_local_high_bit(bucket_index);
            for pair in bucket.get_pairs() {
                let i = Self::get_bucket_index(&pair.a, &dir);
                let new_bucket = if i & bit > 0 { &mut bucket1 } else { &mut bucket0 };
                new_bucket.insert(&pair.a, &pair.b);
            }

            for i in (Self::get_bucket_index(k, &dir) & (bit - 1)..dir_page::PAGE_IDS_SIZE_U32)
                .step_by(bit)
            {
                let new_page_id = if i & bit > 0 { page0_w.id } else { page1_w.id };

                dir.insert(i, new_page_id);
            }

            writep!(dir_page_w, &PageBuf::from(dir));
            writep!(page0_w, &PageBuf::from(&bucket0));
            writep!(page1_w, &PageBuf::from(&bucket0));

            // TODO: mark original page on disk as ready to be allocated
            self.pc.remove_page(bucket_page_w.id);
        }

        Ok(true)
    }

    pub fn remove(&self, k: &K, v: &V) -> crate::Result<bool> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let dir_page_r = dir_page.page.read();
        let dir = Directory::from(&dir_page_r.data);

        let bucket_index = Self::get_bucket_index(k, &dir);
        let bucket_page_id = dir.get(bucket_index);
        let bucket_page = match bucket_page_id {
            0 => return Ok(false),
            _ => self.pc.fetch_page(bucket_page_id)?,
        };
        let mut bucket_page_w = bucket_page.page.write();
        let mut bucket = Bucket::from(&bucket_page_w.data);

        let ret = bucket.remove(k, v);
        writep!(bucket_page_w, &PageBuf::from(bucket));

        // TODO: attempt to merge if empty

        Ok(ret)
    }

    pub fn get(&self, k: &K) -> crate::Result<Vec<V>> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let dir_page_r = dir_page.page.read();
        let dir = Directory::from(&dir_page_r.data);

        let bucket_index = Self::get_bucket_index(k, &dir);
        let bucket_page_id = dir.get(bucket_index);
        let bucket_page = match bucket_page_id {
            0 => return Ok(vec![]),
            _ => self.pc.fetch_page(bucket_page_id)?,
        };

        let bucket_page_w = bucket_page.page.read();
        let bucket = Bucket::from(&bucket_page_w.data);

        Ok(bucket.find(k))
    }

    pub fn get_num_buckets(&self) -> crate::Result<u32> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let dir_page_r = dir_page.page.read();
        let dir = Directory::from(&dir_page_r.data);

        Ok(1 << dir.global_depth())
    }

    fn hash(k: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        k.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn get_bucket_index(k: &K, dir_page: &Directory) -> usize {
        let hash = Self::hash(k);
        let i = hash & dir_page.global_depth_mask();

        i % dir_page::PAGE_IDS_SIZE_U32
    }
}

#[cfg(test)]
mod test {
    use rand::{seq::SliceRandom, thread_rng};

    use crate::{
        disk::Memory,
        hash_table::{bucket_page::BIT_SIZE, dir_page::Directory, extendible::ExtendibleHashTable},
        page::PAGE_SIZE,
        page_cache::PageCache,
        replacer::LRU,
    };

    macro_rules! inserts {
        ($range:expr, $t:ty) => {{
            let mut ret = Vec::with_capacity($range.len());

            let mut keys = $range.collect::<Vec<$t>>();
            keys.shuffle(&mut thread_rng());

            for key in keys {
                let value = key + 10;
                ret.push((key, value));
            }

            ret
        }};
    }

    #[test]
    fn test_extendible_hash_table() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 4;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pm = PageCache::new(disk, replacer, 0);
        let _dir_page = pm.new_page();

        let ht = ExtendibleHashTable::new(0, pm.clone());

        let pairs = 50;
        let inserts = inserts!(-pairs..pairs, i32);

        for (k, v) in &inserts {
            ht.insert(k, v)?;
        }

        let remove = rand::random::<usize>() % inserts.len();
        assert!(ht.remove(&inserts[remove].0, &inserts[remove].1)?);

        let rem = ht.get(&inserts[remove].0)?;
        assert!(rem.is_empty());

        pm.flush_all_pages()?;

        // Make sure it reads back ok
        let ht: ExtendibleHashTable<i32, i32, _> = ExtendibleHashTable::new(0, pm.clone());

        let rem = ht.get(&inserts[remove].0)?;
        assert!(rem.is_empty());

        for (i, (k, v)) in inserts.iter().enumerate() {
            if i == remove {
                continue;
            }

            let r = ht.get(k)?;
            assert!(r[0] == *v);
        }

        Ok(())
    }

    #[test]
    fn test_split() {
        const MEMORY: usize = PAGE_SIZE * 4;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pm = PageCache::new(disk, replacer, 0);
        let ht = ExtendibleHashTable::new(0, pm.clone());

        let _dir_page = pm.new_page();
        assert!(ht.get_num_buckets().unwrap() == 1);

        // (i32, usize) = 12 bytes
        // (4096 - 128) / 12 = 330
        for (k, v) in (0..BIT_SIZE * 8).zip(0..BIT_SIZE * 8).take(330) {
            ht.insert(&k, &v).unwrap();
        }

        assert!(ht.get_num_buckets().unwrap() == 2);

        let dir_page = pm.fetch_page(0).expect("there should be a page 0");
        let dir_page_w = dir_page.page.write();
        let dir = Directory::from(&dir_page_w.data);

        assert!(dir.global_depth() == 1);
    }
}
