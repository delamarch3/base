use std::{
    collections::HashMap,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use tokio::sync::RwLock;

use crate::{
    disk::Disk,
    page::{Page, PageID},
    replacer::{AccessType, LrukReplacer},
};

pub struct BufferPool<const SIZE: usize, const PAGE_SIZE: usize> {
    pages: [MaybeUninit<Arc<RwLock<Page<PAGE_SIZE>>>>; SIZE],
    page_table: HashMap<PageID, usize>,
    free: Vec<usize>,
    disk: Disk,
    next_page_id: AtomicU32,
    replacer: LrukReplacer,
}

impl<const SIZE: usize, const PAGE_SIZE: usize> BufferPool<SIZE, PAGE_SIZE> {
    pub fn new(disk: Disk, replacer: LrukReplacer) -> Self {
        let pages: [MaybeUninit<Arc<RwLock<Page<PAGE_SIZE>>>>; SIZE] =
            std::array::from_fn(|_| MaybeUninit::zeroed());
        let page_table = HashMap::new();
        let free = (0..SIZE).rev().collect();
        let next_page_id = AtomicU32::new(0);

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

    pub async fn new_page<'a>(&mut self) -> Option<Arc<RwLock<Page<PAGE_SIZE>>>> {
        let i = if let Some(i) = self.free.pop() {
            i
        } else {
            // Replacer
            let Some(i) = self.replacer.evict() else { return None };

            let page = unsafe { self.pages[i].assume_init_ref().read().await };
            self.disk.write_page(&page);
            self.page_table.remove(&page.id);

            i
        };

        self.replacer.record_access(i, &AccessType::Get);
        let page_id = self.allocate_page();
        let mut page: Page<PAGE_SIZE> = Page::new(page_id);
        self.disk.write_page(&page);

        page.inc_pin();

        let page = MaybeUninit::new(Arc::new(RwLock::new(page)));
        let page_ref = unsafe { page.assume_init_ref().clone() };
        self.pages[i] = page;

        self.page_table.insert(page_id, i);

        Some(page_ref)
    }

    pub async fn fetch_page<'a>(
        &mut self,
        page_id: PageID,
    ) -> Option<Arc<RwLock<Page<PAGE_SIZE>>>> {
        if let Some(i) = self.page_table.get(&page_id) {
            self.replacer.record_access(*i, &AccessType::Get);
            let page_ref = unsafe { self.pages[*i].assume_init_ref().clone() };
            return Some(page_ref);
        };

        let i = if let Some(i) = self.free.pop() {
            i
        } else {
            // Replacer
            let Some(i) = self.replacer.evict() else { return None };

            let page = unsafe { self.pages[i].assume_init_ref().read().await };
            self.disk.write_page(&page);
            self.page_table.remove(&page.id);

            i
        };

        self.replacer.record_access(i, &AccessType::Get);
        let mut page = self
            .disk
            .read_page::<PAGE_SIZE>(page_id)
            .expect("Couldn't read page");

        page.inc_pin();

        let page = MaybeUninit::new(Arc::new(RwLock::new(page)));
        let page_ref = unsafe { page.assume_init_ref().clone() };
        self.pages[i] = page;

        Some(page_ref)
    }

    pub async fn unpin_page(&self, page_id: PageID) {
        let Some(i) = self.page_table.get(&page_id) else { return };

        unsafe { &self.pages[*i].assume_init_ref().write().await.dec_pin() };
    }

    pub async fn flush_page(&mut self, page_id: PageID) {
        let Some(i) = self.page_table.get(&page_id) else { return };

        let mut page = unsafe { self.pages[*i].assume_init_ref().write().await };
        self.disk.write_page(&page);
        page.set_dirty(false);
        self.replacer.set_evictable(*i, true);
    }

    pub async fn flush_all_pages(&mut self) {
        // Two loops since flush_page is mutable borrow and keys isn't
        let mut page_ids = Vec::with_capacity(self.page_table.len());
        for page_id in self.page_table.keys() {
            page_ids.push(*page_id);
        }

        for page_id in page_ids {
            self.flush_page(page_id).await;
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

        let page_0_id = page_0.read().await.id;
        assert!(page_0_id == 0);

        let tid_1 = page_0.write().await.write_tuple(&Tuple(vec![
            ColumnType::Int32(11),
            ColumnType::String("Tuple 1".into()),
            ColumnType::Float32(1.1),
        ]));
        let tid_2 = page_0.write().await.write_tuple(&Tuple(vec![
            ColumnType::Int32(22),
            ColumnType::String("Tuple 2".into()),
            ColumnType::Float32(2.2),
        ]));

        buf_pool.flush_page(page_0_id).await;

        let page_0 = buf_pool
            .fetch_page(tid_1.0)
            .await
            .expect("should return page 0");
        let page_0_tuples = [
            page_0.read().await.read_tuple(tid_1.1, &schema),
            page_0.read().await.read_tuple(tid_2.1, &schema),
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

        assert!(buf_pool.free.is_empty());
        assert!(buf_pool.page_table.len() == 3);
        assert!(buf_pool.page_table.contains_key(&2));
        assert!(buf_pool.page_table.contains_key(&1));
        assert!(buf_pool.page_table.contains_key(&0));

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

        buf_pool.flush_all_pages().await;

        let _page_3 = buf_pool.new_page().await.expect("should return page 3");

        assert!(buf_pool.page_table.len() == 3);
        assert!(buf_pool.page_table.contains_key(&3));
        assert!(buf_pool.page_table.contains_key(&1));
        assert!(buf_pool.page_table.contains_key(&0));

        // TODO: Arc is heap allocated which is why this is still valid:
        // let page = _page_2.read().await;
        // assert!(page.id == 2);

        Ok(())
    }
}
