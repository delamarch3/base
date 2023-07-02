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

    pub async fn read_page<const SIZE: usize>(&self, page_id: PageID) -> io::Result<Page<SIZE>> {
        let offset = SIZE as i64 * page_id as i64;
        let fd = self.file.as_raw_fd();

        let mut buf = BytesMut::zeroed(SIZE);
        match uio::pread(fd, &mut buf, offset) {
            Ok(n) => eprintln!("Read {n} bytes"),
            Err(e) => panic!("{e}"),
        }

        Ok(Page::from_bytes(page_id, buf))
    }

    pub async fn write_page<const SIZE: usize>(&self, page_id: PageID, page: &Page<SIZE>) {
        let offset = SIZE as i64 * page_id as i64;
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

        let _schema = [Type::Int32, Type::String, Type::Float32];

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
        const DB_FILE: &str = "./test.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::new(DB_FILE).await?;
        let page = get_page();
        let page_id = 0;

        disk.write_page(page_id, &page).await;

        let disk_page = disk.read_page::<DEFAULT_PAGE_SIZE>(page_id).await?;

        assert!(page.data == disk_page.data);

        Ok(())
    }
}
