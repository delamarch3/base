use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering::*};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::disk::Disk;
use crate::page::{
    Page, PageBuf, PageID, PageInner, PageReadGuard2, PageReadGuard3, PageWriteGuard2,
    PageWriteGuard3,
};
use crate::replacer::{AccessType, LRU};

pub const CACHE_SIZE: usize = 64;

pub type FrameID = usize;

pub struct FreeList<const SIZE: usize> {
    free: UnsafeCell<[FrameID; SIZE]>,
    tail: AtomicUsize,
}

unsafe impl<const SIZE: usize> Sync for FreeList<SIZE> {}

impl<const SIZE: usize> Default for FreeList<SIZE> {
    fn default() -> Self {
        let free: UnsafeCell<[FrameID; SIZE]> = UnsafeCell::new(std::array::from_fn(|i| i));

        Self { free, tail: AtomicUsize::new(SIZE) }
    }
}

impl<const SIZE: usize> FreeList<SIZE> {
    pub fn pop(&self) -> Option<FrameID> {
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

    pub fn push(&self, frame_id: FrameID) {
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
    page: &'a Page,
    pub id: PageID,
    i: FrameID,
    replacer: Arc<LRU>,
}

impl Drop for Pin<'_> {
    fn drop(&mut self) {
        self.replacer.unpin(self.i);
    }
}

impl<'a> Pin<'a> {
    pub fn new(page: &'a Page, i: FrameID, id: PageID, replacer: Arc<LRU>) -> Self {
        Self { page, i, id, replacer }
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, PageInner> {
        let w = self.page.write();
        assert!(self.id == w.id, "page was swapped out whilst a pin was held");
        w
    }

    pub fn read(&self) -> RwLockReadGuard<'_, PageInner> {
        self.page.read()
    }

    pub fn read2<T>(&self) -> PageReadGuard2<T> {
        self.page.read2::<T>()
    }

    pub fn write2<T>(&self) -> PageWriteGuard2<T> {
        let w = self.page.write2::<T>();
        assert!(self.id == w.guard.id);
        w
    }

    pub fn read3<T>(&self) -> PageReadGuard3<T>
    where
        T: for<'b> From<&'b PageBuf>,
        PageBuf: for<'b> From<&'b T>,
    {
        self.page.read3::<T>()
    }

    pub fn write3<T>(&self) -> PageWriteGuard3<T>
    where
        T: for<'b> From<&'b PageBuf>,
        PageBuf: for<'b> From<&'b T>,
    {
        self.page.write3::<T>()
    }
}

#[derive(Debug, PartialEq)]
pub enum PageCacheError {
    Disk(std::io::ErrorKind),
    OutOfMemory,
}
pub type Result<T> = std::result::Result<T, PageCacheError>;

pub struct PageCache {
    pages: Box<[Page; CACHE_SIZE]>,
    page_table: RwLock<HashMap<PageID, FrameID>>,
    free: FreeList<CACHE_SIZE>,
    disk: Box<dyn Disk>,
    next_page_id: AtomicI32,
    replacer: Arc<LRU>,
}
pub type SharedPageCache = Arc<PageCache>;

impl PageCache {
    pub fn new<D: Disk + 'static>(disk: D, replacer: Arc<LRU>, next_page_id: PageID) -> Arc<Self> {
        let pages = Box::new(std::array::from_fn(|_| Page::default()));
        let page_table = RwLock::new(HashMap::new());
        let free = FreeList::default();
        let next_page_id = AtomicI32::new(next_page_id);
        let disk = Box::new(disk);

        Arc::new(Self { pages, page_table, free, disk, next_page_id, replacer })
    }

    fn allocate_page(&self) -> PageID {
        self.next_page_id.fetch_add(1, Relaxed)
    }

    pub fn new_page(&self) -> Result<Pin> {
        let page_id = self.allocate_page();

        self.try_get_page(page_id)
    }

    pub fn fetch_page(&self, page_id: PageID) -> Result<Pin> {
        if let Some(i) = self.page_table.read().expect("todo").get(&page_id) {
            let mut replacer = self.replacer.lock();
            replacer.record_access(*i, AccessType::Get);
            replacer.pin(*i);

            return Ok(Pin::new(&self.pages[*i], *i, page_id, self.replacer.clone()));
        };

        self.try_get_page(page_id)
    }

    fn try_get_page(&self, page_id: PageID) -> Result<Pin> {
        let i = match self.free.pop() {
            Some(i) => i,
            None => self.replacer.evict().ok_or(PageCacheError::OutOfMemory)?, // All pages are pinned
        };

        let mut page_w = self.pages[i].write();
        let mut replacer = self.replacer.lock();
        replacer.remove(i);
        replacer.record_access(i, AccessType::Get);
        replacer.pin(i);

        if page_w.dirty {
            self.disk
                .write_page(page_w.id, &page_w.data)
                .map_err(|e| PageCacheError::Disk(e.kind()))?;
        }

        let mut page_table = self.page_table.write().expect("todo");
        page_table.remove(&page_w.id);
        page_table.insert(page_id, i);

        let data = self.disk.read_page(page_id).map_err(|e| PageCacheError::Disk(e.kind()))?;
        page_w.reset();
        page_w.id = page_id;
        page_w.data = data;

        Ok(Pin::new(&self.pages[i], i, page_id, self.replacer.clone()))
    }

    pub fn remove_page(&self, page_id: PageID) {
        use std::collections::hash_map::Entry;
        let i = match self.page_table.write().expect("todo").entry(page_id) {
            Entry::Occupied(entry) => {
                let i = *entry.get();
                entry.remove();
                i
            }
            Entry::Vacant(_) => return,
        };

        self.replacer.remove(i);
        self.free.push(i);
    }

    pub fn flush_page(&self, page_id: PageID) -> Result<()> {
        let page_table = self.page_table.read().expect("todo");
        let Some(i) = page_table.get(&page_id) else {
            return Ok(());
        };

        let mut page_w = self.pages[*i].write();

        self.disk
            .write_page(page_w.id, &page_w.data)
            .map_err(|e| PageCacheError::Disk(e.kind()))?;
        page_w.dirty = false;

        Ok(())
    }

    pub fn flush_all_pages(&self) -> Result<()> {
        for page_id in self.page_table.read().expect("todo").keys() {
            self.flush_page(*page_id)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread};

    use crate::disk::Memory;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::{FreeList, PageCache, PageCacheError, CACHE_SIZE};
    use crate::replacer::LRU;

    #[test]
    fn test_pm_read() -> Result<(), PageCacheError> {
        const MEMORY: usize = PAGE_SIZE * CACHE_SIZE;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pc = PageCache::new(disk, replacer, 0);

        // Hold CACHE_SIZE - 3 pins
        let mut pages = Vec::new();
        for _ in 0..CACHE_SIZE - 2 {
            pages.push(pc.new_page()?)
        }

        // Write to page CACHE_SIZE - 2
        let want = b"test string";
        let id;
        {
            let page = pc.new_page()?;
            id = page.id;
            let mut w = page.write();
            w.put_range(want, 0..want.len());
        }

        // Swap the page out and write something to page PAGE_CACHE - 1 (last available page):
        {
            let data = b"page 8";
            let page = pc.new_page()?;
            let mut w = page.write();
            w.put_range(data, 0..data.len());
        }

        // Read back page CACHE_SIZE - 2
        let page = pc.fetch_page(id)?;
        let r = page.read();
        let have = &r.data[0..want.len()];
        assert!(want == have, "Want: {want:?}, Have: {have:?}");

        Ok(())
    }

    #[test]
    fn test_pm_replacer_full() -> Result<(), PageCacheError> {
        const MEMORY: usize = PAGE_SIZE * CACHE_SIZE;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pc = PageCache::new(disk, replacer, 0);

        let mut pages = Vec::new();
        for _ in 0..CACHE_SIZE {
            pages.push(pc.new_page()?);
        }

        let have = pc.new_page();
        assert!(have.is_err(), "Expected new_page to return OutOfMemory when all pages are pinned");

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
