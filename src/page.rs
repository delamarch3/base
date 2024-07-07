use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[macro_export]
macro_rules! writep {
    ($page:ident, $data:expr) => {
        // TODO: do PageBuf::from in here
        $page.data[..].copy_from_slice($data);
        $page.dirty = true;
    };
    ($page:ident, $range:expr, $data:expr) => {
        $page.data[$range].copy_from_slice($data);
        $page.dirty = true;
    };
}

pub const PAGE_SIZE: usize = 4 * 1024;

pub type PageId = i32;
pub type PageBuf = [u8; PAGE_SIZE];
pub type PageReadGuard<'a> = RwLockReadGuard<'a, PageInner>;
pub type PageWriteGuard<'a> = RwLockWriteGuard<'a, PageInner>;

pub struct Page(RwLock<PageInner>);

impl Default for Page {
    fn default() -> Self {
        let inner = PageInner::default();

        Self(RwLock::new(inner))
    }
}

impl Page {
    pub fn read(&self) -> PageReadGuard {
        self.0.read().expect("todo")
    }

    pub fn write(&self) -> PageWriteGuard {
        self.0.write().expect("todo")
    }
}

pub struct PageInner {
    pub id: PageId,
    pub dirty: bool,
    pub data: PageBuf,
}

impl Default for PageInner {
    fn default() -> Self {
        Self { id: -1, dirty: false, data: [0; PAGE_SIZE] }
    }
}

impl PageInner {
    pub fn reset(&mut self) {
        self.id = 0;
        self.dirty = false;
        self.data.fill(0);
    }
}
