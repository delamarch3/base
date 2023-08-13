use std::{io, os::fd::AsRawFd, path::Path};

use bytes::BytesMut;
use nix::sys::uio;
use tokio::fs::{File, OpenOptions};

use crate::page::{PageID, SharedPage, DEFAULT_PAGE_SIZE};

pub struct Disk<const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    file: File,
}

impl<const PAGE_SIZE: usize> Disk<PAGE_SIZE> {
    pub async fn new(file: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .await?;

        Ok(Self { file })
    }

    pub fn read_page(&self, page_id: PageID) -> io::Result<SharedPage<PAGE_SIZE>> {
        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        let mut buf = BytesMut::zeroed(PAGE_SIZE);
        match uio::pread(fd, &mut buf, offset) {
            Ok(n) => eprintln!("Read {n} bytes"),
            Err(e) => panic!("{e}"),
        }

        Ok(SharedPage::from_bytes(page_id, buf))
    }

    pub fn write_page(&self, page_id: PageID, data: &BytesMut) {
        assert!(data.len() == PAGE_SIZE);

        let offset = PAGE_SIZE as i64 * i64::from(page_id);
        let fd = self.file.as_raw_fd();

        match uio::pwrite(fd, &data, offset) {
            Ok(n) => eprintln!("Written {n} bytes"),
            Err(e) => panic!("{e}"),
        };
    }
}

#[cfg(test)]
mod test {
    use std::io;

    use bytes::BytesMut;

    use crate::{
        page::{PageID, SharedPage, DEFAULT_PAGE_SIZE},
        table_page::{self, ColumnType, Tuple},
        test::CleanUp,
    };

    use super::Disk;

    async fn get_page() -> (PageID, BytesMut) {
        let page: SharedPage<DEFAULT_PAGE_SIZE> = table_page::new_shared(0);

        let tuple_a = Tuple(vec![
            ColumnType::Int32(44),
            ColumnType::String("Hello world".into()),
            ColumnType::Float32(4.4),
        ]);
        let (_page_id, _offset_a) = table_page::write_tuple(&page, &tuple_a).await;

        let tuple_b = Tuple(vec![
            ColumnType::Int32(66),
            ColumnType::String("String".into()),
            ColumnType::Float32(6.6),
        ]);
        let (_page_id, _offset_b) = table_page::write_tuple(&page, &tuple_b).await;

        let page_r = page.read().await;
        (page_r.id, page_r.data.clone())
    }

    #[tokio::test]
    async fn test_disk() -> io::Result<()> {
        const DB_FILE: &str = "./test_disk.db";
        let _cu = CleanUp::file(DB_FILE);
        let disk = Disk::<DEFAULT_PAGE_SIZE>::new(DB_FILE).await?;
        let (id, data) = get_page().await;

        disk.write_page(id, &data);

        let disk_page = disk.read_page(id)?;
        let disk_data = &disk_page.read().await.data;

        assert!(data == disk_data);

        Ok(())
    }
}
