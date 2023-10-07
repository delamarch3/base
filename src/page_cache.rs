use std::{
    cell::UnsafeCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, AtomicUsize, Ordering::*},
        Arc,
    },
};

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    disk::Disk,
    page::{Page, PageId, PageInner},
    replacer::{AccessType, LRUKHandle},
};

pub const CACHE_SIZE: usize = 8;

pub type FrameId = usize;

pub struct FreeList<const SIZE: usize> {
    free: UnsafeCell<[FrameId; SIZE]>,
    tail: AtomicUsize,
}

unsafe impl<const SIZE: usize> Sync for FreeList<SIZE> {}

impl<const SIZE: usize> Default for FreeList<SIZE> {
    fn default() -> Self {
        let free: UnsafeCell<[FrameId; SIZE]> = UnsafeCell::new(std::array::from_fn(|i| i));

        Self {
            free,
            tail: AtomicUsize::new(SIZE),
        }
    }
}

impl<const SIZE: usize> FreeList<SIZE> {
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
            if tail == SIZE {
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

    pub async fn write(&self) -> RwLockWriteGuard<'_, PageInner> {
        self.page.write().await
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, PageInner> {
        self.page.read().await
    }
}

pub type SharedPageCache = Arc<PageCache>;

pub struct PageCache {
    pages: [Page; CACHE_SIZE],
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free: FreeList<CACHE_SIZE>,
    disk: Disk,
    next_page_id: AtomicI32,
    replacer: LRUKHandle,
}

impl PageCache {
    pub fn new(disk: Disk, replacer: LRUKHandle, next_page_id: PageId) -> Arc<Self> {
        let pages = std::array::from_fn(|_| Page::default());
        let page_table = RwLock::new(HashMap::new());
        let free = FreeList::default();
        let next_page_id = AtomicI32::new(next_page_id);

        Arc::new(Self {
            pages,
            page_table,
            free,
            disk,
            next_page_id,
            replacer,
        })
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
        use std::collections::hash_map::Entry;
        let i = match self.page_table.write().await.entry(page_id) {
            Entry::Occupied(entry) => {
                let i = *entry.get();
                entry.remove();
                i
            }
            Entry::Vacant(_) => return,
        };

        self.replacer.remove(i).await;
        self.free.push(i);
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
    use std::{io, sync::Arc, thread};

    use crate::{
        disk::Disk,
        page_cache::{FreeList, PageCache},
        replacer::LRUKHandle,
        test::CleanUp,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pm_replacer() -> io::Result<()> {
        const DB_FILE: &str = "./test_pm_replacer.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;

        let replacer = LRUKHandle::new(2);
        let pc = PageCache::new(disk, replacer, 0);

        {
            let _p0 = pc.new_page().await.expect("should return page 0"); // id = 0 ts = 0
            let _p1 = pc.new_page().await.expect("should return page 1"); // id = 1 ts = 1
            let _p2 = pc.new_page().await.expect("should return page 2"); // id = 2 ts = 2

            let _p3 = pc.new_page().await.expect("should return page 3");
            let _p4 = pc.new_page().await.expect("should return page 4");
            let _p5 = pc.new_page().await.expect("should return page 5");
            let _p6 = pc.new_page().await.expect("should return page 6");
            let _p7 = pc.new_page().await.expect("should return page 7");

            let inner = pc.clone();
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

        // Page 2 was accessed the least and should have the largest k distance of 7
        // Page 1 should have a k distance of 3
        // Page 0 should have a k distance of 1

        let _p8 = pc.new_page().await.expect("should return page 8");

        let inner = &pc;
        let page_table = inner.page_table.read().await;
        assert!(page_table.contains_key(&8));
        assert!(!page_table.contains_key(&2));

        Ok(())
    }

    #[test]
    fn test_free_list() {
        thread::scope(|s| {
            const SIZE: usize = 8;
            let list = Arc::new(FreeList::<SIZE>::default());

            // Pop
            let list_a = list.clone();
            let a = s.spawn(move || {
                let mut got = vec![];
                for _ in 0..SIZE / 2 {
                    got.push(list_a.pop().unwrap());
                }

                got
            });

            let list_b = list.clone();
            let b = s.spawn(move || {
                let mut got = vec![];
                for _ in 4..SIZE {
                    got.push(list_b.pop().unwrap());
                }

                got
            });

            let mut got = a.join().unwrap();
            let got_b = b.join().unwrap();

            got.extend(&got_b);
            got.sort();

            assert!(got == vec![0, 1, 2, 3, 4, 5, 6, 7], "Got: {got:?}");

            // Push
            let list_c = list.clone();
            let c = s.spawn(move || {
                for i in 4..8 {
                    list_c.push(i);
                }
            });

            let list_d = list.clone();
            let d = s.spawn(move || {
                for i in 8..12 {
                    list_d.push(i);
                }
            });

            c.join().unwrap();
            d.join().unwrap();

            let mut got = unsafe { *list.free.get() };
            got.sort();

            assert!(got == [4, 5, 6, 7, 8, 9, 10, 11]);
        });
    }
}
