use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use crate::catalog::schema::Schema;
use crate::hash_table::bucket::Bucket;
use crate::hash_table::directory::{self, Directory};
use crate::page::{PageBuf, PageID};
use crate::page_cache::SharedPageCache;
use crate::storable::Storable;
use crate::table::tuple::Data as TupleData;
use crate::writep;

pub struct ExtendibleHashTable<'a, V> {
    dir_page_id: PageID,
    pc: SharedPageCache,
    schema: &'a Schema,
    _data: PhantomData<V>,
}

impl<'a, V> ExtendibleHashTable<'a, V>
where
    V: Storable + Copy + Eq,
{
    pub fn new(dir_page_id: PageID, pc: SharedPageCache, schema: &'a Schema) -> Self {
        Self { dir_page_id, pc, schema, _data: PhantomData }
    }

    pub fn insert(&self, key: TupleData, value: V) -> crate::Result<bool> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let mut dir_page_w = dir_page.page.write();
        let mut dir = Directory::from(&dir_page_w.data);

        let bucket_index = Self::get_bucket_index(&key, &dir);
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
        let mut bucket = Bucket::deserialise_page(&bucket_page_w.data, &self.schema);

        bucket.insert(key, value);
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
            let mut bucket0 = Bucket::deserialise_page(&page0_w.data, &self.schema);

            let page1 = self.pc.new_page()?;
            let mut page1_w = page1.page.write();
            let mut bucket1 = Bucket::deserialise_page(&page1_w.data, &self.schema);

            let bit = dir.get_local_high_bit(bucket_index);
            for pair in bucket.get_pairs() {
                let i = Self::get_bucket_index(&pair.a, &dir);
                let new_bucket = if i & bit > 0 { &mut bucket1 } else { &mut bucket0 };
                new_bucket.insert(pair.a, pair.b);
            }

            for i in (bucket_index & (bit - 1)..directory::PAGE_IDS_SIZE_U32).step_by(bit) {
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

    pub fn remove(&self, key: &TupleData, v: &V) -> crate::Result<bool> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let dir_page_r = dir_page.page.read();
        let dir = Directory::from(&dir_page_r.data);

        let bucket_index = Self::get_bucket_index(key, &dir);
        let bucket_page_id = dir.get(bucket_index);
        let bucket_page = match bucket_page_id {
            0 => return Ok(false),
            _ => self.pc.fetch_page(bucket_page_id)?,
        };
        let mut bucket_page_w = bucket_page.page.write();
        let mut bucket = Bucket::deserialise_page(&bucket_page_w.data, &self.schema);

        let ret = bucket.remove(key, v);
        writep!(bucket_page_w, &PageBuf::from(bucket));

        // TODO: attempt to merge if empty

        Ok(ret)
    }

    pub fn get(&self, key: &TupleData) -> crate::Result<Vec<V>> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let dir_page_r = dir_page.page.read();
        let dir = Directory::from(&dir_page_r.data);

        let bucket_index = Self::get_bucket_index(key, &dir);
        let bucket_page_id = dir.get(bucket_index);
        let bucket_page = match bucket_page_id {
            0 => return Ok(vec![]),
            _ => self.pc.fetch_page(bucket_page_id)?,
        };

        let bucket_page_w = bucket_page.page.read();
        let bucket = Bucket::deserialise_page(&bucket_page_w.data, &self.schema);

        Ok(bucket.find(key))
    }

    pub fn get_num_buckets(&self) -> crate::Result<u32> {
        let dir_page = self.pc.fetch_page(self.dir_page_id)?;
        let dir_page_r = dir_page.page.read();
        let dir = Directory::from(&dir_page_r.data);

        Ok(1 << dir.global_depth())
    }

    fn hash(key: &TupleData) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn get_bucket_index(key: &TupleData, directory: &Directory) -> usize {
        let hash = Self::hash(key);
        let i = hash & directory.global_depth_mask();

        i % directory::PAGE_IDS_SIZE_U32
    }
}

#[cfg(test)]
mod test {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use crate::disk::Memory;
    use crate::hash_table::bucket::BITMAP_SIZE;
    use crate::hash_table::directory::Directory;
    use crate::hash_table::extendible::ExtendibleHashTable;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::table::tuple::{Builder as TupleBuilder, Data as TupleData};
    use crate::{column, schema};

    #[test]
    fn test_extendible_hash_table() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 4;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pm = PageCache::new(disk, replacer, 0);
        let dir = pm.new_page()?;

        let key_schema = schema! { column!("c1", Int) };
        let table = ExtendibleHashTable::new(dir.id, pm.clone(), &key_schema);

        const BOUND: i32 = 50;
        let mut pairs: Vec<(TupleData, i32)> = (-BOUND..BOUND)
            .into_iter()
            .map(|n| (TupleBuilder::new().int(n).build(), n + 10))
            .collect();
        pairs.shuffle(&mut thread_rng());

        for (key, value) in &pairs {
            table.insert(key.clone(), value.clone())?;
        }

        let remove = rand::random::<usize>() % pairs.len();
        assert!(table.remove(&pairs[remove].0, &pairs[remove].1)?);

        let rem = table.get(&pairs[remove].0)?;
        assert!(rem.is_empty());

        pm.flush_all_pages()?;

        // Make sure it reads back ok
        let table: ExtendibleHashTable<i32> =
            ExtendibleHashTable::new(dir.id, pm.clone(), &key_schema);

        let rem = table.get(&pairs[remove].0)?;
        assert!(rem.is_empty());

        for (i, (k, v)) in pairs.iter().enumerate() {
            if i == remove {
                continue;
            }

            let r = table.get(k)?;
            assert_eq!(r[0], *v);
        }

        Ok(())
    }

    #[test]
    fn test_split() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 4;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pm = PageCache::new(disk, replacer, 0);
        let dir = pm.new_page()?;

        let key_schema = schema! { column!("c1", Int) };
        let table = ExtendibleHashTable::new(dir.id, pm.clone(), &key_schema);

        assert_eq!(table.get_num_buckets().unwrap(), 1);

        // (key = i32, value = usize) = 12 bytes
        // (4096 - 128) / 12 = 330
        for (k, v) in (0..BITMAP_SIZE as i32 * 8)
            .zip(0..BITMAP_SIZE * 8)
            .take(330)
            .map(|(n, value)| (TupleBuilder::new().int(n).build(), value))
        {
            table.insert(k, v).unwrap();
        }

        assert_eq!(table.get_num_buckets().unwrap(), 2);

        let dir_page = pm.fetch_page(0).expect("there should be a page 0");
        let dir_page_w = dir_page.page.write();
        let dir = Directory::from(&dir_page_w.data);

        assert_eq!(dir.global_depth(), 1);

        Ok(())
    }
}
