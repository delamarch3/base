use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering::*},
        Arc,
    },
};

use tokio::sync::{Mutex, RwLock};

use crate::{
    disk::Disk,
    page::{Page, PageId},
    replacer::{AccessType, LRUKReplacer},
};

pub type FrameId = usize;

#[derive(Clone)]
pub struct PageCache<const SIZE: usize>(Arc<PageCacheInner<SIZE>>);

impl<const SIZE: usize> PageCache<SIZE> {
    pub fn new(disk: Disk, replacer: LRUKReplacer, next_page_id: PageId) -> Self {
        let inner = Arc::new(PageCacheInner::new(disk, replacer, next_page_id));

        Self(inner)
    }

    pub async fn new_page<'a>(&self) -> Option<&Page> {
        self.0.new_page().await
    }

    pub async fn fetch_page<'a>(&self, page_id: PageId) -> Option<&Page> {
        self.0.fetch_page(page_id).await
    }

    pub async fn unpin_page(&self, page_id: PageId) {
        self.0.unpin_page(page_id).await
    }

    pub async fn flush_page(&self, page_id: PageId) {
        self.0.flush_page(page_id).await
    }

    pub async fn flush_all_pages(&self) {
        self.0.flush_all_pages().await
    }
}

pub struct FreeList<const SIZE: usize> {
    free: [FrameId; SIZE],
    tail: usize,
}

impl<const SIZE: usize> FreeList<SIZE> {
    pub fn new() -> Self {
        let free: [FrameId; SIZE] = std::array::from_fn(|mut i| {
            let ret = i;
            i += 1;
            ret
        });

        Self { free, tail: SIZE }
    }

    pub fn pop(&mut self) -> Option<FrameId> {
        if self.tail == 0 {
            return None;
        }

        let ret = self.free[self.tail - 1];
        self.tail -= 1;

        Some(ret)
    }

    pub fn push(&mut self, frame_id: FrameId) {
        if self.tail == SIZE {
            eprintln!("warn: trying to push frame to full free list");
        }

        self.tail += 1;
        self.free[self.tail] = frame_id;
    }

    pub fn is_empty(&self) -> bool {
        self.tail == 0
    }
}

struct PageCacheInner<const SIZE: usize> {
    pages: [Page; SIZE],
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free: Mutex<FreeList<SIZE>>,
    disk: Disk,
    next_page_id: AtomicU32,
    replacer: Mutex<LRUKReplacer>,
}

impl<const SIZE: usize> PageCacheInner<SIZE> {
    pub fn new(disk: Disk, replacer: LRUKReplacer, next_page_id: PageId) -> Self {
        let pages = std::array::from_fn(|_| Page::default());
        let page_table = RwLock::new(HashMap::new());
        let free = Mutex::new(FreeList::new());
        let replacer = Mutex::new(replacer);
        let next_page_id = AtomicU32::new(next_page_id);

        Self {
            pages,
            page_table,
            free,
            disk,
            next_page_id,
            replacer,
        }
    }

    fn allocate_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, SeqCst)
    }

    pub async fn new_page<'a>(&self) -> Option<&Page> {
        let i = if let Some(i) = self.free.lock().await.pop() {
            i
        } else {
            let Some(i) = self.replacer.lock().await.evict() else { return None };

            let page_r = &self.pages[i].read().await;

            self.disk.write_page(page_r.id, &page_r.data);
            self.page_table.write().await.remove(&page_r.id);

            i
        };

        self.replacer
            .lock()
            .await
            .record_access(i, &AccessType::Get);

        let page_id = self.allocate_page();
        let page = &self.pages[i];
        let mut page_w = page.write().await;
        self.disk.write_page(page_id, &page_w.data);

        page_w.reset();
        page_w.id = page_id;

        self.page_table.write().await.insert(page_id, i);

        Some(&self.pages[i])
    }

    pub async fn fetch_page<'a>(&self, page_id: PageId) -> Option<&Page> {
        if let Some(i) = self.page_table.read().await.get(&page_id) {
            self.replacer
                .lock()
                .await
                .record_access(*i, &AccessType::Get);

            self.pages[*i].write().await.pin += 1;

            return Some(&self.pages[*i]);
        };

        let i = if let Some(i) = self.free.lock().await.pop() {
            i
        } else {
            let Some(i) = self.replacer.lock().await.evict() else { return None };

            let page_r = self.pages[i].read().await;

            self.disk.write_page(page_r.id, &page_r.data);
            self.page_table.write().await.remove(&page_id);

            i
        };

        self.replacer
            .lock()
            .await
            .record_access(i, &AccessType::Get);
        let data = self.disk.read_page(page_id).expect("Couldn't read page");

        let mut page_w = self.pages[i].write().await;
        page_w.reset();
        page_w.id = page_id;
        page_w.data = data;

        Some(&self.pages[i])
    }

    pub async fn unpin_page(&self, page_id: PageId) {
        let page_table = self.page_table.read().await;
        let Some(i) = page_table.get(&page_id) else { return };

        let mut page_w = self.pages[*i].write().await;
        if page_w.pin > 0 {
            page_w.pin -= 1;
        }
        if page_w.pin == 0 {
            self.replacer.lock().await.set_evictable(*i, true);
        }
    }

    pub async fn flush_page(&self, page_id: PageId) {
        let page_table = self.page_table.read().await;
        let Some(i) = page_table.get(&page_id) else { return };

        let mut page_w = self.pages[*i].write().await;
        self.disk.write_page(page_w.id, &page_w.data);
        page_w.dirty = false;
    }

    pub async fn flush_all_pages(&self) {
        for page_id in self.page_table.read().await.keys() {
            self.flush_page(*page_id).await;
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::{disk::Disk, page_manager::PageCache, replacer::LRUKReplacer, test::CleanUp};

    #[tokio::test]
    async fn test_pm_replacer() -> io::Result<()> {
        const DB_FILE: &str = "./test_pm_replacer.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let replacer = LRUKReplacer::new(2);
        let pc: PageCache<3> = PageCache::new(disk, replacer, 0);

        let _page_0 = pc.new_page().await.expect("should return page 0"); // id = 0 ts = 0
        let _page_1 = pc.new_page().await.expect("should return page 1"); // id = 1 ts = 1
        let _page_2 = pc.new_page().await.expect("should return page 2"); // id = 2 ts = 2

        let inner = pc.0.clone();
        assert!(inner.free.lock().await.is_empty());
        assert!(inner.page_table.read().await.len() == 3);
        assert!(inner.page_table.read().await.contains_key(&2));
        assert!(inner.page_table.read().await.contains_key(&1));
        assert!(inner.page_table.read().await.contains_key(&0));
        drop(inner);

        pc.fetch_page(0).await; // ts = 3
        pc.fetch_page(0).await; // ts = 4

        pc.fetch_page(1).await; // ts = 5

        pc.fetch_page(0).await; // ts = 6
        pc.fetch_page(0).await; // ts = 7

        pc.fetch_page(1).await; // ts = 8

        pc.fetch_page(2).await; // ts = 9

        // Page 2 is the least regularly accessed and should have the largest k distance of 7
        // Page 1 should have a k distance of 3
        // Page 0 should have a k distance of 1

        // Unpin pages so they can be evicted:
        pc.unpin_page(0).await;
        pc.unpin_page(1).await;
        pc.unpin_page(2).await;
        // TODO: debug this
        pc.unpin_page(2).await;

        let _page_3 = pc.new_page().await.expect("should return page 3");

        let inner = pc.0;
        assert!(inner.page_table.read().await.len() == 3);
        assert!(inner.page_table.read().await.contains_key(&3));
        assert!(inner.page_table.read().await.contains_key(&1));
        assert!(inner.page_table.read().await.contains_key(&0));

        Ok(())
    }
}
