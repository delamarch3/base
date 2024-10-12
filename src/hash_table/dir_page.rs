use std::ops::Range;

use crate::page::{PageBuf, PageID, PAGE_SIZE};

pub const PAGE_IDS_SIZE_U32: usize = 512;
pub const PAGE_IDS_SIZE_U8: usize = 512 * 4;

const GLOBAL_DEPTH: Range<usize> = 0..4;
const LOCAL_DEPTHS: Range<usize> = 4..4 + PAGE_IDS_SIZE_U32;
const PAGE_IDS: Range<usize> = PAGE_IDS_SIZE_U32..PAGE_IDS_SIZE_U32 + PAGE_IDS_SIZE_U8;

#[derive(Debug)]
pub struct Directory {
    global_depth: u32,
    /// Local depth for each page
    local_depths: [u8; PAGE_IDS_SIZE_U32],
    /// Bucket page IDs
    page_ids: [u8; PAGE_IDS_SIZE_U8],
}

impl From<&PageBuf> for Directory {
    fn from(buf: &PageBuf) -> Self {
        let global_depth = u32::from_be_bytes(buf[GLOBAL_DEPTH].try_into().unwrap());

        let mut local_depths = [0; PAGE_IDS_SIZE_U32];
        local_depths[..].copy_from_slice(&buf[LOCAL_DEPTHS]);

        let mut bucket_page_ids = [0; PAGE_IDS_SIZE_U8];
        bucket_page_ids[..].copy_from_slice(&buf[PAGE_IDS]);

        Self { global_depth, local_depths, page_ids: bucket_page_ids }
    }
}

impl From<&Directory> for PageBuf {
    fn from(dir: &Directory) -> Self {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[GLOBAL_DEPTH].copy_from_slice(&dir.global_depth.to_be_bytes());
        ret[LOCAL_DEPTHS].copy_from_slice(&dir.local_depths);
        ret[PAGE_IDS].copy_from_slice(&dir.page_ids);

        ret
    }
}

impl From<Directory> for PageBuf {
    fn from(dir: Directory) -> Self {
        Self::from(&dir)
    }
}

impl Directory {
    pub fn get(&self, i: usize) -> PageID {
        i32::from_be_bytes(self.page_ids[i * 4..(i * 4) + 4].try_into().unwrap())
    }

    pub fn insert(&mut self, i: usize, id: PageID) {
        self.page_ids[i * 4..(i * 4) + 4].copy_from_slice(&i32::to_be_bytes(id));
    }

    pub fn set_global_depth(&mut self, depth: u32) {
        self.global_depth = depth;
    }

    pub fn incr_global_depth(&mut self) {
        self.global_depth += 1;
    }

    pub fn local_depth_mask(&self, i: usize) -> usize {
        Self::depth_mask(self.local_depths[i] as u32)
    }

    pub fn global_depth_mask(&self) -> usize {
        Self::depth_mask(self.global_depth)
    }

    pub fn get_local_high_bit(&self, i: usize) -> usize {
        1 << self.local_depths[i]
    }

    #[inline]
    fn depth_mask(depth: u32) -> usize {
        // 0 => ...0001
        // 1 => ...0011
        // 2 => ...0111
        // etc

        (1 << depth) - 1
    }

    pub fn global_depth(&self) -> u32 {
        self.global_depth
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hash_table::dir_page::Directory,
        page::{Page, PageBuf, PAGE_SIZE},
        writep,
    };

    #[test]
    fn test_depth_mask() {
        let mut dir = Directory::from(&[0; PAGE_SIZE]);

        assert!(dir.global_depth_mask() == 0);

        dir.set_global_depth(2);
        assert!(dir.global_depth_mask() == 3);

        dir.set_global_depth(4);
        assert!(dir.global_depth_mask() == 15);

        dir.set_global_depth(8);
        assert!(dir.global_depth_mask() == 255);
    }

    #[test]
    fn test_directory() {
        let page = Page::default();
        let mut w = page.write();

        let mut dir = Directory::from(&w.data);

        dir.insert(1, 1);
        dir.insert(2, 2);
        dir.insert(10, 10);

        assert!(dir.get(1) == 1);
        assert!(dir.get(2) == 2);
        assert!(dir.get(10) == 10);

        writep!(w, &PageBuf::from(dir));

        // Make sure it reads back ok
        let dir = Directory::from(&w.data);
        assert!(dir.get(1) == 1);
        assert!(dir.get(2) == 2);
        assert!(dir.get(10) == 10);
    }
}
