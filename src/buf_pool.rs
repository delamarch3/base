use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use tokio::sync::{Mutex, RwLock};

use crate::{
    disk::Disk,
    page::{Page, PageID},
    replacer::{AccessType, LrukReplacer},
};

#[derive(Clone)]
pub struct BufferPool<const SIZE: usize, const PAGE_SIZE: usize> {
    pages: Arc<RwLock<[Option<Page<PAGE_SIZE>>; SIZE]>>,
    page_table: Arc<RwLock<HashMap<PageID, usize>>>,
    free: Arc<RwLock<Vec<usize>>>,
    disk: Arc<Mutex<Disk<PAGE_SIZE>>>,
    next_page_id: Arc<AtomicU32>,
    replacer: Arc<RwLock<LrukReplacer>>,
}

impl<const SIZE: usize, const PAGE_SIZE: usize> BufferPool<SIZE, PAGE_SIZE> {
    pub fn new(disk: Disk<PAGE_SIZE>, replacer: LrukReplacer) -> Self {
        let pages = Arc::new(RwLock::new(std::array::from_fn(|_| None)));
        let page_table = Arc::new(RwLock::new(HashMap::new()));
        let free = Arc::new(RwLock::new((0..SIZE).rev().collect()));
        let next_page_id = Arc::new(AtomicU32::new(0));
        let disk = Arc::new(Mutex::new(disk));
        let replacer = Arc::new(RwLock::new(replacer));

        Self {
            pages,
            page_table,
            free,
            disk,
            next_page_id,
            replacer,
        }
    }

    fn allocate_page(&self) -> PageID {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    pub async fn new_page<'a>(&mut self) -> Option<Page<PAGE_SIZE>> {
        let i = if let Some(i) = self.free.write().await.pop() {
            i
        } else {
            // Replacer
            let Some(i) = self.replacer.write().await.evict() else { return None };

            let pages = self.pages.read().await;
            if let Some(page) = &pages[i] {
                let page_id = page.get_id().await;
                let data = page.get_data().await;

                self.disk.lock().await.write_page(page_id, data);
                self.page_table.write().await.remove(&page_id);
            }

            i
        };

        self.replacer
            .write()
            .await
            .record_access(i, &AccessType::Get);

        let page_id = self.allocate_page();
        let page: Page<PAGE_SIZE> = Page::new(page_id);
        self.disk
            .lock()
            .await
            .write_page(page_id, page.get_data().await);
        page.inc_pin().await;

        let mut pages = self.pages.write().await;
        pages[i].replace(page.clone());

        self.page_table.write().await.insert(page_id, i);

        Some(page)
    }

    pub async fn fetch_page<'a>(&mut self, page_id: PageID) -> Option<Page<PAGE_SIZE>> {
        if let Some(i) = self.page_table.read().await.get(&page_id) {
            self.replacer
                .write()
                .await
                .record_access(*i, &AccessType::Get);

            if let Some(page) = &self.pages.read().await[*i] {
                return Some(page.clone());
            }
        };

        let i = if let Some(i) = self.free.write().await.pop() {
            i
        } else {
            // Replacer
            let Some(i) = self.replacer.write().await.evict() else { return None };

            if let Some(page) = self.pages.read().await[i].clone() {
                let page_id = page.get_id().await;
                let data = page.get_data().await;

                self.disk.lock().await.write_page(page_id, data);
                self.page_table.write().await.remove(&page_id);
            }

            i
        };

        self.replacer
            .write()
            .await
            .record_access(i, &AccessType::Get);
        let page = self
            .disk
            .lock()
            .await
            .read_page(page_id)
            .expect("Couldn't read page");
        page.inc_pin().await;

        let mut pages = self.pages.write().await;
        pages[i].replace(page.clone());

        Some(page)
    }

    pub async fn unpin_page(&mut self, page_id: PageID) {
        let page_table = self.page_table.read().await;
        let Some(i) = page_table.get(&page_id) else { return };

        // let mut page = unsafe { self.pages[*i].assume_init_mut().write().await };
        if let Some(page) = &self.pages.read().await[*i] {
            if page.dec_pin().await == 0 {
                self.replacer.write().await.set_evictable(*i, true);
            }
        }
    }

    pub async fn flush_page(&self, page_id: PageID) {
        let page_table = self.page_table.read().await;
        let Some(i) = page_table.get(&page_id) else { return };

        if let Some(page) = &self.pages.read().await[*i] {
            let page_id = page.get_id().await;
            let data = page.get_data().await;

            self.disk.lock().await.write_page(page_id, data);
            page.set_dirty(false).await;
        }
    }

    pub async fn flush_all_pages(&mut self) {
        for page_id in self.page_table.read().await.keys() {
            self.flush_page(*page_id).await;
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::{
        buf_pool::BufferPool,
        disk::Disk,
        page::{ColumnType, Tuple, Type, DEFAULT_PAGE_SIZE},
        replacer::LrukReplacer,
        test::CleanUp,
    };

    #[tokio::test]
    async fn test_buf_pool_rw_page() -> io::Result<()> {
        const DB_FILE: &str = "./test_buf_pool_rw_page.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let replacer = LrukReplacer::new(2);
        let mut buf_pool: BufferPool<4, DEFAULT_PAGE_SIZE> = BufferPool::new(disk, replacer);

        let schema = [Type::Int32, Type::String, Type::Float32];
        let expected_tuples = [
            Tuple(vec![
                ColumnType::Int32(11),
                ColumnType::String("Tuple 1".into()),
                ColumnType::Float32(1.1),
            ]),
            Tuple(vec![
                ColumnType::Int32(22),
                ColumnType::String("Tuple 2".into()),
                ColumnType::Float32(2.2),
            ]),
        ];

        let page_0 = buf_pool.new_page().await.expect("should return page 0");

        let page_0_id = page_0.get_id().await;
        assert!(page_0_id == 0);

        let tid_1 = page_0
            .write_tuple(&Tuple(vec![
                ColumnType::Int32(11),
                ColumnType::String("Tuple 1".into()),
                ColumnType::Float32(1.1),
            ]))
            .await;
        let tid_2 = page_0
            .write_tuple(&Tuple(vec![
                ColumnType::Int32(22),
                ColumnType::String("Tuple 2".into()),
                ColumnType::Float32(2.2),
            ]))
            .await;

        buf_pool.flush_page(page_0_id).await;

        let page_0 = buf_pool
            .fetch_page(tid_1.0)
            .await
            .expect("should return page 0");
        let page_0_tuples = [
            page_0.read_tuple(tid_1.1, &schema).await,
            page_0.read_tuple(tid_2.1, &schema).await,
        ];

        assert!(page_0_tuples == expected_tuples);

        Ok(())
    }

    #[tokio::test]
    async fn test_buf_pool_replacer() -> io::Result<()> {
        const DB_FILE: &str = "./test_buf_pool_replacer.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let replacer = LrukReplacer::new(2);
        let mut buf_pool: BufferPool<3, DEFAULT_PAGE_SIZE> = BufferPool::new(disk, replacer);

        let _page_0 = buf_pool.new_page().await.expect("should return page 0"); // id = 0 ts = 0
        let _page_1 = buf_pool.new_page().await.expect("should return page 1"); // id = 1 ts = 1
        let _page_2 = buf_pool.new_page().await.expect("should return page 2"); // id = 2 ts = 2

        let page_table = buf_pool.page_table.read().await;
        assert!(buf_pool.free.read().await.is_empty());
        assert!(page_table.len() == 3);
        assert!(page_table.contains_key(&2));
        assert!(page_table.contains_key(&1));
        assert!(page_table.contains_key(&0));
        drop(page_table);

        buf_pool.fetch_page(0).await; // ts = 3
        buf_pool.fetch_page(0).await; // ts = 4

        buf_pool.fetch_page(1).await; // ts = 5

        buf_pool.fetch_page(0).await; // ts = 6
        buf_pool.fetch_page(0).await; // ts = 7

        buf_pool.fetch_page(1).await; // ts = 8

        buf_pool.fetch_page(2).await; // ts = 9

        // Page 2 is the least regularly accessed and should have the largest k distance of 7
        // Page 1 should have a k distance of 3
        // Page 0 should have a k distance of 1

        // Unpin pages so they can be evicted:
        buf_pool.unpin_page(0).await;
        buf_pool.unpin_page(1).await;
        buf_pool.unpin_page(2).await;

        let _page_3 = buf_pool.new_page().await.expect("should return page 3");

        let page_table = buf_pool.page_table.read().await;
        assert!(page_table.len() == 3);
        assert!(page_table.contains_key(&3));
        assert!(page_table.contains_key(&1));
        assert!(page_table.contains_key(&0));

        Ok(())
    }
}
