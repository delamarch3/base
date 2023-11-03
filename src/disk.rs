use std::{
    cell::UnsafeCell,
    io,
    os::fd::AsRawFd,
    path::Path,
    sync::atomic::{AtomicBool, Ordering::*},
};

use nix::sys::uio;
use tokio::fs::{File, OpenOptions};

use crate::page::{PageId, PAGE_SIZE};

pub trait Disk {
    fn read_page(&self, page_id: PageId) -> io::Result<[u8; PAGE_SIZE]>;
    fn write_page(&self, page_id: PageId, data: &[u8; PAGE_SIZE]);
}

pub struct FileSystem {
    file: File,
}

impl Disk for FileSystem {
    fn read_page(&self, page_id: PageId) -> io::Result<[u8; PAGE_SIZE]> {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        let mut buf = [0; PAGE_SIZE];
        match uio::pread(fd, &mut buf, offset) {
            Ok(_) => {}
            Err(e) => panic!("{e}"),
        }

        Ok(buf)
    }

    fn write_page(&self, page_id: PageId, data: &[u8; PAGE_SIZE]) {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        match uio::pwrite(fd, data, offset) {
            Ok(_) => {}
            Err(e) => panic!("{e}"),
        };
    }
}

impl FileSystem {
    pub async fn new(file: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .await?;

        Ok(Self { file })
    }
}

pub struct Memory {
    buf: UnsafeCell<Box<[u8]>>,
    size: usize,
}

unsafe impl Send for Memory {}
unsafe impl Sync for Memory {}

impl Disk for Memory {
    fn read_page(&self, page_id: PageId) -> io::Result<[u8; PAGE_SIZE]> {
        let offset = PAGE_SIZE * page_id as usize;
        assert!(offset <= self.size - PAGE_SIZE);

        let buf = unsafe { &*self.buf.get() };
        let mut ret = [0; PAGE_SIZE];
        ret.copy_from_slice(&buf[offset..offset + PAGE_SIZE]);

        Ok(ret)
    }

    fn write_page(&self, page_id: PageId, data: &[u8; PAGE_SIZE]) {
        let offset = PAGE_SIZE * page_id as usize;
        assert!(offset <= self.size - PAGE_SIZE);

        let buf = unsafe { &mut *self.buf.get() };
        buf[offset..offset + PAGE_SIZE].copy_from_slice(data);
    }
}

impl Memory {
    pub fn new<const SIZE: usize>() -> Self {
        assert!(SIZE % PAGE_SIZE == 0);

        Self {
            buf: UnsafeCell::new(Box::new([0; SIZE])),
            size: SIZE,
        }
    }
}
