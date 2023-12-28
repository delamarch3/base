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
    disk::{Disk, FileSystem},
    page::{Page, PageId, PageInner},
    replacer::{AccessType, LRUKHandle},
};

pub const CACHE_SIZE: usize = 32;

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
        let mut tail = self.tail.load(Relaxed);
        let mut new_tail;
        loop {
            if tail == 0 {
                return None;
            }

            new_tail = tail - 1;
            match self.tail.compare_exchange(tail, new_tail, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(t) => tail = t,
            };
        }

        unsafe { Some((*self.free.get())[new_tail]) }
    }

    pub fn push(&self, frame_id: FrameId) {
        let mut tail = self.tail.load(Relaxed);
        let mut new_tail;
        loop {
            assert!(tail != SIZE);

            new_tail = tail + 1;
            match self.tail.compare_exchange(tail, new_tail, Relaxed, Relaxed) {
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
    pub id: PageId,
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
    pub fn new(page: &'a Page, i: FrameId, id: PageId, replacer: LRUKHandle) -> Self {
        Self {
            page,
            i,
            id,
            replacer,
        }
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, PageInner> {
        let w = self.page.write().await;

        assert!(self.id == w.id, "page was swapped out whilst a pin was held");

        w
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, PageInner> {
        self.page.read().await
    }
}

pub type SharedPageCache<D> = Arc<PageCache<D>>;

pub struct PageCache<D: Disk = FileSystem> {
    pages: Box<[Page; CACHE_SIZE]>,
    page_table: RwLock<HashMap<PageId, FrameId>>,
    free: FreeList<CACHE_SIZE>,
    disk: D,
    next_page_id: AtomicI32,
    replacer: LRUKHandle,
}

impl<D: Disk> PageCache<D> {
    pub fn new(disk: D, replacer: LRUKHandle, next_page_id: PageId) -> Arc<Self> {
        // Workaround to allocate pages since std::array::from_fn(|_| Page::default()) overflows
        // the stack:
        let mut pages;
        unsafe {
            let layout = std::alloc::Layout::new::<[Page; CACHE_SIZE]>();
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }

            pages = Box::from_raw(ptr as *mut [Page; CACHE_SIZE]);

            for page in pages.iter_mut() {
                *page = Page::default();
            }
        };

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
        self.next_page_id.fetch_add(1, Relaxed)
    }

    pub async fn new_page<'a>(&self) -> Option<Pin> {
        let page_id = self.allocate_page();

        self.try_get_page(page_id).await
    }

    pub async fn fetch_page<'a>(&self, page_id: PageId) -> Option<Pin> {
        if let Some(i) = self.page_table.read().await.get(&page_id) {
            self.replacer.record_access(*i, AccessType::Get).await;
            self.replacer.pin(*i).await;

            return Some(Pin::new(&self.pages[*i], *i, page_id, self.replacer.clone()));
        };

        self.try_get_page(page_id).await
    }

    // TODO: should return Result
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

        Some(Pin::new(&self.pages[i], i, page_id, self.replacer.clone()))
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
        disk::Memory,
        page::PAGE_SIZE,
        page_cache::{FreeList, PageCache},
        replacer::LRUKHandle,
        writep,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_pm_read() -> io::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 16;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRUKHandle::new(2);
        let pc = PageCache::new(disk, replacer, 0);

        // Hold 7 pins (pages 0 to 6):
        let _pages = tokio::join!(
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
        );

        // Write to page 7
        let want = b"test string";
        let id;
        {
            let page = pc.new_page().await.unwrap();
            id = page.id;
            let mut w = page.write().await;
            w.data[0..want.len()].copy_from_slice(want);
            w.dirty = true;
        }

        // Swap the page out and write something else:
        {
            let data = b"page 8";
            let page = pc.new_page().await.unwrap();
            let mut w = page.write().await;
            writep!(w, 0..data.len(), data);
        }

        // Read back page 7
        let page = pc.fetch_page(id).await.unwrap();
        let r = page.read().await;
        let have = &r.data[0..want.len()];
        assert!(want == have, "Want: {want:?}, Have: {have:?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test_pm_replacer_full() -> io::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 8;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRUKHandle::new(2);
        let pc = PageCache::new(disk, replacer, 0);

        let _pages = tokio::join!(
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page(),
            pc.new_page()
        );

        let have = pc.new_page().await;
        assert!(have.is_none(), "Expected new_page to return None when replacer is full");

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
