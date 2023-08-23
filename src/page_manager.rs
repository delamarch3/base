use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    disk::Disk,
    page::{PageID, SharedPage},
    replacer::{AccessType, LrukReplacer},
};

#[derive(Clone)]
pub struct BufferPool<const SIZE: usize, const PAGE_SIZE: usize> {
    inner: Arc<Mutex<Inner<SIZE, PAGE_SIZE>>>,
}

impl<const SIZE: usize, const PAGE_SIZE: usize> BufferPool<SIZE, PAGE_SIZE> {
    pub fn new(disk: Disk<PAGE_SIZE>, replacer: LrukReplacer, next_page_id: PageID) -> Self {
        let inner = Arc::new(Mutex::new(Inner::new(disk, replacer, next_page_id)));

        Self { inner }
    }

    pub async fn new_page<'a>(&self) -> Option<SharedPage<PAGE_SIZE>> {
        self.inner.lock().await.new_page().await
    }

    pub async fn fetch_page<'a>(&self, page_id: PageID) -> Option<SharedPage<PAGE_SIZE>> {
        self.inner.lock().await.fetch_page(page_id).await
    }

    pub async fn unpin_page(&self, page_id: PageID) {
        self.inner.lock().await.unpin_page(page_id).await
    }

    pub async fn flush_page(&self, page_id: PageID) {
        self.inner.lock().await.flush_page(page_id).await
    }

    pub async fn flush_all_pages(&self) {
        self.inner.lock().await.flush_all_pages().await
    }
}

struct Inner<const SIZE: usize, const PAGE_SIZE: usize> {
    pages: [Option<SharedPage<PAGE_SIZE>>; SIZE],
    page_table: HashMap<PageID, usize>,
    free: Vec<usize>,
    disk: Disk<PAGE_SIZE>,
    next_page_id: PageID,
    replacer: LrukReplacer,
}

impl<const SIZE: usize, const PAGE_SIZE: usize> Inner<SIZE, PAGE_SIZE> {
    pub fn new(disk: Disk<PAGE_SIZE>, replacer: LrukReplacer, next_page_id: PageID) -> Self {
        let pages = std::array::from_fn(|_| None);
        let page_table = HashMap::new();
        let free = (0..SIZE).rev().collect();

        Self {
            pages,
            page_table,
            free,
            disk,
            next_page_id,
            replacer,
        }
    }

    fn allocate_page(&mut self) -> PageID {
        let ret = self.next_page_id;
        self.next_page_id += 1;

        ret
    }

    pub async fn new_page<'a>(&mut self) -> Option<SharedPage<PAGE_SIZE>> {
        let i = if let Some(i) = self.free.pop() {
            i
        } else {
            // Replacer
            let Some(i) = self.replacer.evict() else { return None };

            if let Some(page) = &self.pages[i] {
                let page_r = page.read().await;

                self.disk.write_page(page_r.id, &page_r.data);
                self.page_table.remove(&page_r.id);
            }

            i
        };

        self.replacer.record_access(i, &AccessType::Get);

        let page_id = self.allocate_page();
        let page: SharedPage<PAGE_SIZE> = SharedPage::new(page_id);

        let mut page_w = page.write().await;
        self.disk.write_page(page_id, &page_w.data);
        page_w.pin += 1;
        drop(page_w);

        self.pages[i].replace(page.clone());

        self.page_table.insert(page_id, i);

        Some(page)
    }

    pub async fn fetch_page<'a>(&mut self, page_id: PageID) -> Option<SharedPage<PAGE_SIZE>> {
        if let Some(i) = self.page_table.get(&page_id) {
            self.replacer.record_access(*i, &AccessType::Get);

            if let Some(page) = &self.pages[*i] {
                page.write().await.pin += 1;

                return Some(page.clone());
            }
        };

        let i = if let Some(i) = self.free.pop() {
            i
        } else {
            // Replacer
            let Some(i) = self.replacer.evict() else { return None };

            if let Some(page) = self.pages[i].clone() {
                let page_r = page.read().await;

                self.disk.write_page(page_r.id, &page_r.data);
                self.page_table.remove(&page_id);
            }

            i
        };

        self.replacer.record_access(i, &AccessType::Get);
        let page = self.disk.read_page(page_id).expect("Couldn't read page");
        page.write().await.pin += 1;

        self.pages[i].replace(page.clone());

        Some(page)
    }

    pub async fn unpin_page(&mut self, page_id: PageID) {
        let Some(i) = self.page_table.get(&page_id) else { return };

        if let Some(page) = &self.pages[*i] {
            let mut page_w = page.write().await;
            page_w.pin -= 1;
            if page_w.pin == 0 {
                self.replacer.set_evictable(*i, true);
            }
        }
    }

    pub async fn flush_page(&self, page_id: PageID) {
        let Some(i) = self.page_table.get(&page_id) else { return };

        if let Some(page) = &self.pages[*i] {
            let mut page_w = page.write().await;

            self.disk.write_page(page_w.id, &page_w.data);
            page_w.dirty = false;
        }
    }

    pub async fn flush_all_pages(&self) {
        for page_id in self.page_table.keys() {
            self.flush_page(*page_id).await;
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::{
        disk::Disk,
        page::DEFAULT_PAGE_SIZE,
        page_manager::BufferPool,
        replacer::LrukReplacer,
        table_page::{self, ColumnType, Tuple, Type},
        test::CleanUp,
    };

    // TODO: should be a table_page test
    #[tokio::test]
    async fn test_buf_pool_rw_page() -> io::Result<()> {
        const DB_FILE: &str = "./test_buf_pool_rw_page.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let replacer = LrukReplacer::new(2);
        let buf_pool: BufferPool<4, DEFAULT_PAGE_SIZE> = BufferPool::new(disk, replacer, 0);

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
        let page_0_id = 0;

        table_page::init(page_0.write().await);
        let tid_1 = table_page::write_tuple(
            &page_0,
            &Tuple(vec![
                ColumnType::Int32(11),
                ColumnType::String("Tuple 1".into()),
                ColumnType::Float32(1.1),
            ]),
        )
        .await;

        let tid_2 = table_page::write_tuple(
            &page_0,
            &Tuple(vec![
                ColumnType::Int32(22),
                ColumnType::String("Tuple 2".into()),
                ColumnType::Float32(2.2),
            ]),
        )
        .await;

        buf_pool.flush_page(page_0_id).await;

        let page_0 = buf_pool
            .fetch_page(tid_1.0)
            .await
            .expect("should return page 0");
        let page_0_tuples = [
            table_page::read_tuple(&page_0, tid_1.1, &schema).await,
            table_page::read_tuple(&page_0, tid_2.1, &schema).await,
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
        let buf_pool: BufferPool<3, DEFAULT_PAGE_SIZE> = BufferPool::new(disk, replacer, 0);

        let _page_0 = buf_pool.new_page().await.expect("should return page 0"); // id = 0 ts = 0
        let _page_1 = buf_pool.new_page().await.expect("should return page 1"); // id = 1 ts = 1
        let _page_2 = buf_pool.new_page().await.expect("should return page 2"); // id = 2 ts = 2

        let inner = buf_pool.inner.lock().await;
        assert!(inner.free.is_empty());
        assert!(inner.page_table.len() == 3);
        assert!(inner.page_table.contains_key(&2));
        assert!(inner.page_table.contains_key(&1));
        assert!(inner.page_table.contains_key(&0));
        drop(inner);

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
        // TODO: debug this
        buf_pool.unpin_page(2).await;

        let _page_3 = buf_pool.new_page().await.expect("should return page 3");

        let inner = buf_pool.inner.lock().await;
        assert!(inner.page_table.len() == 3);
        assert!(inner.page_table.contains_key(&3));
        assert!(inner.page_table.contains_key(&1));
        assert!(inner.page_table.contains_key(&0));

        Ok(())
    }
}
