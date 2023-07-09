use std::{io, os::fd::AsRawFd, path::Path};

use bytes::BytesMut;
use nix::sys::uio;
use tokio::fs::{File, OpenOptions};

use crate::page::{Page, PageID};

pub struct Disk {
    file: File,
}

impl Disk {
    pub async fn new(file: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .await?;

        Ok(Self { file })
    }

    pub fn read_page<const SIZE: usize>(&self, page_id: PageID) -> io::Result<Page<SIZE>> {
        // let offset = SIZE as i64 * page_id as i64;
        let offset = SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        let mut buf = BytesMut::zeroed(SIZE);
        match uio::pread(fd, &mut buf, offset) {
            Ok(n) => eprintln!("Read {n} bytes"),
            Err(e) => panic!("{e}"),
        }

        Ok(Page::from_bytes(page_id, buf))
    }

    pub fn write_page<const SIZE: usize>(&self, page: &Page<SIZE>) {
        let offset = SIZE as i64 * i64::from(page.id);
        let fd = self.file.as_raw_fd();

        match uio::pwrite(fd, &page.data, offset) {
            Ok(n) => eprintln!("Written {n} bytes"),
            Err(e) => panic!("{e}"),
        };
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use crate::{
        page::{ColumnType, Page, Tuple, Type, DEFAULT_PAGE_SIZE},
        test::CleanUp,
    };

    use super::Disk;

    fn get_page() -> Page<DEFAULT_PAGE_SIZE> {
        let mut page: Page<DEFAULT_PAGE_SIZE> = Page::new(0);

        let tuple_a = Tuple(vec![
            ColumnType::Int32(44),
            ColumnType::String("Hello world".into()),
            ColumnType::Float32(4.4),
        ]);
        let (_page_id, _offset_a) = page.write_tuple(&tuple_a);

        let tuple_b = Tuple(vec![
            ColumnType::Int32(66),
            ColumnType::String("String".into()),
            ColumnType::Float32(6.6),
        ]);
        let (_page_id, _offset_b) = page.write_tuple(&tuple_b);

        page
    }

    #[tokio::test]
    async fn test_disk() -> io::Result<()> {
        const DB_FILE: &str = "./test_disk.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;
        let page = get_page();

        disk.write_page(&page);

        let disk_page = disk.read_page::<DEFAULT_PAGE_SIZE>(page.id)?;

        assert!(page.data == disk_page.data);

        Ok(())
    }
}
