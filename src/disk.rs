use std::{cell::UnsafeCell, io, os::fd::AsRawFd, path::Path};

use nix::sys::uio;
use tokio::fs::{File, OpenOptions};

use crate::page::{PageBuf, PageId, PAGE_SIZE};

pub trait Disk {
    fn read_page(&self, page_id: PageId) -> io::Result<PageBuf>;
    fn write_page(&self, page_id: PageId, data: &PageBuf) -> io::Result<()>;
}

pub struct FileSystem {
    file: File,
}

impl Disk for FileSystem {
    fn read_page(&self, page_id: PageId) -> io::Result<PageBuf> {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();
        let mut buf = [0; PAGE_SIZE];
        uio::pread(fd, &mut buf, offset)?;

        Ok(buf)
    }

    fn write_page(&self, page_id: PageId, data: &PageBuf) -> io::Result<()> {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        uio::pwrite(fd, data, offset)?;

        Ok(())
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
    fn read_page(&self, page_id: PageId) -> io::Result<PageBuf> {
        let offset = PAGE_SIZE * page_id as usize;
        assert!(offset <= self.size - PAGE_SIZE);

        let buf = unsafe { &*self.buf.get() };
        let mut ret = [0; PAGE_SIZE];
        ret.copy_from_slice(&buf[offset..offset + PAGE_SIZE]);

        Ok(ret)
    }

    fn write_page(&self, page_id: PageId, data: &PageBuf) -> io::Result<()> {
        let offset = PAGE_SIZE * page_id as usize;
        assert!(offset <= self.size - PAGE_SIZE);

        let buf = unsafe { &mut *self.buf.get() };
        buf[offset..offset + PAGE_SIZE].copy_from_slice(data);

        Ok(())
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
