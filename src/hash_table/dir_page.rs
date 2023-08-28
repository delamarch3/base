use std::mem::size_of;

use tokio::sync::RwLockWriteGuard;

use crate::{
    copy_bytes, get_u32,
    page::{Page, PageID, DEFAULT_PAGE_SIZE},
    put_bytes,
};

pub const DEFAULT_BUCKET_PAGE_IDS_SIZE: usize = 512;
pub const DEFAULT_BUCKET_PAGE_IDS_SIZE_U8: usize = 512;

#[derive(Debug)]
pub struct Directory<const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    global_depth: u32,
    local_depths: [u8; DEFAULT_BUCKET_PAGE_IDS_SIZE],
    bucket_page_ids: [u8; DEFAULT_BUCKET_PAGE_IDS_SIZE_U8],
}

impl<const PAGE_SIZE: usize> Directory<PAGE_SIZE> {
    pub fn new(data: &[u8; PAGE_SIZE]) -> Self {
        let global_depth = get_u32!(data, 0);
        let mut local_depths = [0; DEFAULT_BUCKET_PAGE_IDS_SIZE];
        copy_bytes!(
            local_depths,
            data,
            size_of::<u32>(),
            DEFAULT_BUCKET_PAGE_IDS_SIZE
        );
        let mut bucket_page_ids = [0; DEFAULT_BUCKET_PAGE_IDS_SIZE_U8];
        copy_bytes!(
            bucket_page_ids,
            data,
            DEFAULT_BUCKET_PAGE_IDS_SIZE,
            DEFAULT_BUCKET_PAGE_IDS_SIZE_U8
        );

        Self {
            global_depth,
            local_depths,
            bucket_page_ids,
        }
    }

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>) {
        put_bytes!(
            page.data,
            self.global_depth.to_be_bytes(),
            0,
            size_of::<u32>()
        );
        put_bytes!(
            page.data,
            self.local_depths,
            size_of::<u32>(),
            DEFAULT_BUCKET_PAGE_IDS_SIZE
        );
        put_bytes!(
            page.data,
            self.bucket_page_ids,
            DEFAULT_BUCKET_PAGE_IDS_SIZE,
            DEFAULT_BUCKET_PAGE_IDS_SIZE_U8
        );

        page.dirty = true;
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

    pub fn set_local_depth(&mut self, i: usize, depth: u8) {
        self.local_depths[i] = depth;
    }

    pub fn incr_local_depth(&mut self, i: usize) {
        self.local_depths[i] += 1;
    }

    pub fn decr_local_depth(&mut self, i: usize) {
        self.local_depths[i] -= 1;
    }

    pub fn set_global_depth(&mut self, depth: u32) {
        self.global_depth = depth;
    }

    pub fn incr_global_depth(&mut self) {
        self.global_depth += 1;
    }

    pub fn decr_global_depth(&mut self) {
        self.global_depth += 1;
    }

    pub fn get_local_depth_mask(&self, i: usize) -> usize {
        Self::get_depth_mask(self.local_depths[i] as u32)
    }

    pub fn get_global_depth_mask(&self) -> usize {
        Self::get_depth_mask(self.global_depth)
    }

    pub fn get_local_high_bit(&self, i: usize) -> usize {
        1 << self.local_depths[i]
    }

    #[inline]
    fn get_depth_mask(depth: u32) -> usize {
        // ld = 0 => ...0000
        // ld = 1 => ...0001
        // ld = 2 => ...0011
        // etc

        (1 << depth) - 1
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table::dir_page::Directory,
        page::{SharedPage, DEFAULT_PAGE_SIZE},
    };

    #[test]
    fn test_depth_mask() {
        let mut dir = Directory::new(&[0; DEFAULT_PAGE_SIZE]);

        assert!(dir.get_global_depth_mask() == 0);

        dir.set_global_depth(2);
        assert!(dir.get_global_depth_mask() == 3);

        dir.set_global_depth(4);
        assert!(dir.get_global_depth_mask() == 15);

        dir.set_global_depth(8);
        assert!(dir.get_global_depth_mask() == 255);
    }

    #[tokio::test]
    async fn test_directory() {
        let page = SharedPage::<DEFAULT_PAGE_SIZE>::new(0);
        let mut page_w = page.write().await;

        let mut dir = Directory::new(&page_w.data);

        dir.set_page_id(1, 1);
        dir.set_page_id(2, 2);
        dir.set_page_id(10, 10);

        assert!(dir.get_page_id(1) == 1);
        assert!(dir.get_page_id(2) == 2);
        assert!(dir.get_page_id(10) == 10);

        dir.write_data(&mut page_w);

        drop(dir);

        // Make sure it reads back ok
        let dir = Directory::new(&page_w.data);
        assert!(dir.get_page_id(1) == 1);
        assert!(dir.get_page_id(2) == 2);
        assert!(dir.get_page_id(10) == 10);
    }
}
