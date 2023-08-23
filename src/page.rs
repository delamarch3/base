use std::sync::Arc;

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

pub type PageID = u32;
pub const DEFAULT_PAGE_SIZE: usize = 4 * 1024;

#[derive(Clone)]
pub struct SharedPage<const SIZE: usize = DEFAULT_PAGE_SIZE> {
    id: PageID,
    inner: Arc<RwLock<Page<SIZE>>>,
}

impl<const SIZE: usize> SharedPage<SIZE> {
    pub fn new(id: PageID) -> Self {
        let page = Page {
            id,
            pin: 0,
            dirty: false,
            data: [0; SIZE],
        };

        let inner = Arc::new(RwLock::new(page));

        Self { id, inner }
    }

    pub fn from_bytes(id: PageID, data: [u8; SIZE]) -> Self {
        let inner = Arc::new(RwLock::new(Page {
            id,
            pin: 0,
            dirty: false,
            data,
        }));

        Self { id, inner }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, Page<SIZE>> {
        self.inner.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, Page<SIZE>> {
        self.inner.write().await
    }

    pub fn get_id(&self) -> PageID {
        self.id
    }
}

pub struct Page<const SIZE: usize = DEFAULT_PAGE_SIZE> {
    pub id: PageID,
    pub pin: u32,
    pub dirty: bool,
    pub data: [u8; SIZE],
}

impl<const SIZE: usize> Page<SIZE> {
    pub fn reset_data(&mut self) {
        self.data = [0; SIZE]
    }
}
