use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering::*},
        Arc,
    },
};

use tokio::sync::{Mutex, RwLock};

use crate::{
    disk::Disk,
    page::{Page, PageId},
    replacer::{AccessType, LRUKHandle},
};

pub const CACHE_SIZE: usize = 8;

pub type FrameId = usize;

pub struct FreeList {
    free: UnsafeCell<[FrameId; CACHE_SIZE]>,
    tail: AtomicUsize,
}

impl Default for FreeList {
    fn default() -> Self {
        let free: UnsafeCell<[FrameId; CACHE_SIZE]> = UnsafeCell::new(std::array::from_fn(|i| i));

        Self {
            free,
            tail: AtomicUsize::new(CACHE_SIZE),
        }
    }
}

impl FreeList {
    pub fn pop(&self) -> Option<FrameId> {
        let mut tail = self.tail.load(SeqCst);
        let mut new_tail;
        loop {
            if tail == 0 {
                return None;
            }

            new_tail = tail - 1;
            match self.tail.compare_exchange(tail, new_tail, SeqCst, Relaxed) {
                Ok(_) => break,
                Err(t) => tail = t,
            };
        }

        unsafe { Some((*self.free.get())[new_tail]) }
    }

    pub fn push(&self, frame_id: FrameId) {
        let mut tail = self.tail.load(SeqCst);
        let mut new_tail;
        loop {
            if tail == CACHE_SIZE {
                panic!("trying to push frame to full free list");
            }

            new_tail = tail + 1;
            match self.tail.compare_exchange(tail, new_tail, SeqCst, Relaxed) {
                Ok(_) => break,
                Err(t) => tail = t,
            }
        }

        unsafe { (*self.free.get())[new_tail - 1] = frame_id }
    }

    pub fn is_empty(&self) -> bool {
        self.tail.load(Relaxed) == 0
    }

    pub fn len(&self) -> usize {
        self.tail.load(Relaxed)
    }
}

pub struct Pin<'a> {
    pub page: &'a Page,
    i: FrameId,
    replacer: LRUKHandle,
}

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            self.replacer.blocking_unpin(self.i);
        });
    }
}

impl<'a> Pin<'a> {
    pub fn new(page: &'a Page, i: FrameId, replacer: LRUKHandle) -> Self {
        Self { page, i, replacer }
    }
}

#[derive(Clone)]
pub struct PageCache(Arc<PageCacheInner>);

impl PageCache {
    pub fn new(disk: Disk, replacer: LRUKHandle, next_page_id: PageId) -> Self {
        let inner = Arc::new(PageCacheInner::new(disk, replacer, next_page_id));

        Self(inner)
    }

    pub async fn new_page<'a>(&self) -> Option<Pin> {
        self.0.new_page().await
    }

    pub async fn fetch_page<'a>(&self, page_id: PageId) -> Option<Pin> {
        self.0.fetch_page(page_id).await
    }

    pub async fn flush_page(&self, page_id: PageId) {
        self.0.flush_page(page_id).await
    }

    pub async fn flush_all_pages(&self) {
        self.0.flush_all_pages().await
    }
}

struct PageCacheInner {
    pages: [Page; CACHE_SIZE],
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free: FreeList,
    disk: Disk,
    next_page_id: AtomicI32,
    replacer: LRUKHandle,
}

impl PageCacheInner {
    pub fn new(disk: Disk, replacer: LRUKHandle, next_page_id: PageId) -> Self {
        let pages = std::array::from_fn(|_| Page::default());
        let page_table = RwLock::new(HashMap::new());
        let free = FreeList::default();
        let next_page_id = AtomicI32::new(next_page_id);

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

    pub async fn new_page<'a>(&self) -> Option<Pin> {
        let page_id = self.allocate_page();

        self.try_get_page(page_id).await
    }

    pub async fn fetch_page<'a>(&self, page_id: PageId) -> Option<Pin> {
        if let Some(i) = self.page_table.read().await.get(&page_id) {
            self.replacer.record_access(*i, AccessType::Get).await;
            self.replacer.pin(*i).await;

            return Some(Pin::new(&self.pages[*i], *i, self.replacer.clone()));
        };

        self.try_get_page(page_id).await
    }

    async fn try_get_page(&self, page_id: PageId) -> Option<Pin> {
        let i = match self.free.pop() {
            Some(i) => i,
            None => self.replacer.evict().await?,
        };

        let mut page_w = self.pages[i].write().await;
        self.replacer.remove(i).await;
        self.replacer.record_access(i, AccessType::Get).await;
        self.replacer.pin(i).await;

        if page_w.dirty {
            self.disk.write_page(page_w.id, &page_w.data);
        }

        let mut page_table = self.page_table.write().await;
        page_table.remove(&page_w.id);
        page_table.insert(page_id, i);

        let data = self.disk.read_page(page_id).expect("Couldn't read page");
        page_w.reset();
        page_w.id = page_id;
        page_w.data = data;

        Some(Pin::new(&self.pages[i], i, self.replacer.clone()))
    }

    pub async fn remove_page(&self, page_id: PageId) {
        // Check page table
        // Remove from replacer
        // Add to free list
        todo!()
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

    use crate::{disk::Disk, page_cache::PageCache, replacer::LRUKHandle, test::CleanUp};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pm_replacer() -> io::Result<()> {
        const DB_FILE: &str = "./test_pm_replacer.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let replacer = LRUKHandle::new(2);
        let pc: PageCache = PageCache::new(disk, replacer, 0);

        {
            let _p0 = pc.new_page().await.expect("should return page 0"); // id = 0 ts = 0
            let _p1 = pc.new_page().await.expect("should return page 1"); // id = 1 ts = 1
            let _p2 = pc.new_page().await.expect("should return page 2"); // id = 2 ts = 2

            let _p3 = pc.new_page().await.expect("should return page 3");
            let _p4 = pc.new_page().await.expect("should return page 4");
            let _p5 = pc.new_page().await.expect("should return page 5");
            let _p6 = pc.new_page().await.expect("should return page 6");
            let _p7 = pc.new_page().await.expect("should return page 7");

            let inner = pc.0.clone();
            let page_table = inner.page_table.read().await;
            assert!(inner.free.is_empty());
            assert!(page_table.contains_key(&2));
            assert!(page_table.contains_key(&1));
            assert!(page_table.contains_key(&0));
            drop(page_table);
            drop(inner);

            pc.fetch_page(0).await; // ts = 3
            pc.fetch_page(0).await; // ts = 4

            pc.fetch_page(1).await; // ts = 5

            pc.fetch_page(0).await; // ts = 6
            pc.fetch_page(0).await; // ts = 7

            pc.fetch_page(1).await; // ts = 8

            pc.fetch_page(2).await; // ts = 9
        }

        // Page 2 is the least regularly accessed and should have the largest k distance of 7
        // Page 1 should have a k distance of 3
        // Page 0 should have a k distance of 1

        let _page_3 = pc.new_page().await.expect("should return page 3");

        let inner = &pc.0;
        let page_table = inner.page_table.read().await;
        assert!(page_table.contains_key(&3));
        assert!(page_table.contains_key(&1));
        assert!(page_table.contains_key(&0));

        Ok(())
    }
}
