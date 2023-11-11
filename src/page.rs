use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[macro_export]
macro_rules! get_u64 {
    ($src:expr, $o:expr) => {
        u64::from_be_bytes($src[$o as usize..$o as usize + 8].try_into().unwrap())
    };
}

#[macro_export]
macro_rules! get_u32 {
    ($src:expr, $o:expr) => {
        u32::from_be_bytes($src[$o as usize..$o as usize + 4].try_into().unwrap())
    };
}

#[macro_export]
macro_rules! get_i32 {
    ($src:expr, $o:expr) => {
        i32::from_be_bytes($src[$o as usize..$o as usize + 4].try_into().unwrap())
    };
}

#[macro_export]
macro_rules! get_bytes {
    ($src:expr, $o:expr, $l:expr) => {
        &$src[$o as usize..$o as usize + $l as usize]
    };
}

#[macro_export]
macro_rules! put_bytes {
    ($dst:expr, $src:expr, $o:expr, $l:expr) => {
        $dst[$o as usize..$o as usize + $l as usize].copy_from_slice(&$src);
    };
}

#[macro_export]
macro_rules! copy_bytes {
    ($dst:expr, $src:expr, $o:expr, $l:expr) => {
        $dst[..].copy_from_slice(&$src[$o as usize..$o as usize + $l as usize])
    };
}

/// Crates a [u8; _] from &[u8]
#[macro_export]
macro_rules! byte_array {
    ($t:ty, $src:expr) => {{
        let mut bytes = [0; size_of::<$t>()];
        bytes[..].copy_from_slice(&$src[0..size_of::<$t>()]);
        bytes
    }};
    ($t:ty, $src:expr, $o:expr) => {{
        let mut bytes = [0; size_of::<$t>()];
        bytes[..].copy_from_slice(&$src[$o as usize..$o as usize + size_of::<$t>()]);
        bytes
    }};
}

pub const PAGE_SIZE: usize = 4 * 1024;

pub type PageId = i32;
pub type PageBuf = [u8; PAGE_SIZE];

pub struct Page(RwLock<PageInner>);

impl Default for Page {
    fn default() -> Self {
        let inner = PageInner::default();

        Self(RwLock::new(inner))
    }
}

impl Page {
    pub async fn read(&self) -> RwLockReadGuard<'_, PageInner> {
        self.0.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, PageInner> {
        self.0.write().await
    }
}

pub struct PageInner {
    pub id: PageId,
    pub dirty: bool,
    pub data: [u8; PAGE_SIZE],
}

impl Default for PageInner {
    fn default() -> Self {
        Self {
            id: -1,
            dirty: false,
            data: [0; PAGE_SIZE],
        }
    }
}

impl PageInner {
    pub fn reset(&mut self) {
        self.id = 0;
        self.dirty = false;
        self.data.fill(0);
    }
}
