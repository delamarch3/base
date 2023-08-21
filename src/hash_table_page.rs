use std::mem::size_of;

use bytes::BytesMut;
use tokio::sync::RwLockWriteGuard;

use crate::{
    get_bytes, get_u32,
    page::{Page, PageID, DEFAULT_PAGE_SIZE},
    put_bytes,
};

pub const LOCAL_DEPTHS_SIZE: usize = 512;
pub const BUCKET_PAGE_IDS_SIZE: usize = 2048;

#[derive(Debug)]
pub struct Directory<const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    global_depth: u32,
    local_depths: BytesMut,
    bucket_page_ids: BytesMut,
}

impl<const PAGE_SIZE: usize> Directory<PAGE_SIZE> {
    pub const SIZE: usize = LOCAL_DEPTHS_SIZE + BUCKET_PAGE_IDS_SIZE + size_of::<u32>();

    pub fn write(page: &RwLockWriteGuard<'_, Page<PAGE_SIZE>>) -> Self {
        let data = &page.data;

        let global_depth = get_u32!(data, 0);
        let local_depths = BytesMut::from(get_bytes!(data, size_of::<u32>(), LOCAL_DEPTHS_SIZE));
        let bucket_page_ids =
            BytesMut::from(get_bytes!(data, LOCAL_DEPTHS_SIZE, BUCKET_PAGE_IDS_SIZE));

        Self {
            global_depth,
            local_depths,
            bucket_page_ids,
        }
    }

    pub fn as_bytes(&self) -> BytesMut {
        let mut ret = BytesMut::zeroed(PAGE_SIZE);

        put_bytes!(ret, self.global_depth.to_be_bytes(), 0, size_of::<u32>());
        put_bytes!(ret, self.local_depths, size_of::<u32>(), LOCAL_DEPTHS_SIZE);
        put_bytes!(
            ret,
            self.bucket_page_ids,
            LOCAL_DEPTHS_SIZE,
            BUCKET_PAGE_IDS_SIZE
        );

        ret
    }

    pub fn get_page_id(&self, i: usize) -> PageID {
        get_u32!(self.bucket_page_ids, i * size_of::<u32>())
    }

    pub fn set_page_id(&mut self, i: usize, id: PageID) {
        put_bytes!(
            self.bucket_page_ids,
            u32::to_be_bytes(id),
            i * size_of::<u32>(),
            size_of::<u32>()
        );
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table_page::Directory,
        page::{SharedPage, DEFAULT_PAGE_SIZE},
    };

    #[tokio::test]
    async fn test_directory() {
        let page = SharedPage::<DEFAULT_PAGE_SIZE>::new(0);
        let mut page_w = page.write().await;

        let mut dir = Directory::write(&page_w);

        dir.set_page_id(1, 1);
        dir.set_page_id(2, 2);
        dir.set_page_id(10, 10);

        assert!(dir.get_page_id(1) == 1);
        assert!(dir.get_page_id(2) == 2);
        assert!(dir.get_page_id(10) == 10);

        let dir_bytes = dir.as_bytes();
        assert!(dir_bytes.len() == DEFAULT_PAGE_SIZE);
        page_w.data = dir_bytes;

        drop(dir);

        // Make sure it reads back ok
        let dir = Directory::write(&page_w);
        assert!(dir.get_page_id(1) == 1);
        assert!(dir.get_page_id(2) == 2);
        assert!(dir.get_page_id(10) == 10);
    }
}
