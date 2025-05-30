use std::ops::Range;

use crate::page::{DiskObject, PageBuf, PageID, PAGE_SIZE};

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

impl Default for Directory {
    fn default() -> Self {
        Self {
            global_depth: 0,
            local_depths: [0; PAGE_IDS_SIZE_U32],
            page_ids: [0; PAGE_IDS_SIZE_U8],
        }
    }
}

impl DiskObject for Directory {
    fn serialise(&self) -> PageBuf {
        let mut buf: PageBuf = [0; PAGE_SIZE];

        buf[GLOBAL_DEPTH].copy_from_slice(&self.global_depth.to_be_bytes());
        buf[LOCAL_DEPTHS].copy_from_slice(&self.local_depths);
        buf[PAGE_IDS].copy_from_slice(&self.page_ids);

        buf
    }

    fn deserialise(buf: PageBuf, _: &crate::catalog::schema::Schema) -> Self {
        let global_depth = u32::from_be_bytes(buf[GLOBAL_DEPTH].try_into().unwrap());

        let mut local_depths = [0; PAGE_IDS_SIZE_U32];
        local_depths[..].copy_from_slice(&buf[LOCAL_DEPTHS]);

        let mut bucket_page_ids = [0; PAGE_IDS_SIZE_U8];
        bucket_page_ids[..].copy_from_slice(&buf[PAGE_IDS]);

        Self { global_depth, local_depths, page_ids: bucket_page_ids }
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
        depth_mask(self.local_depths[i] as u32)
    }

    pub fn global_depth_mask(&self) -> usize {
        depth_mask(self.global_depth)
    }

    pub fn get_local_high_bit(&self, i: usize) -> usize {
        1 << self.local_depths[i]
    }

    pub fn global_depth(&self) -> u32 {
        self.global_depth
    }
}

#[inline]
fn depth_mask(depth: u32) -> usize {
    // 0 => ...0001
    // 1 => ...0011
    // 2 => ...0111
    // etc
    (1 << depth) - 1
}

#[cfg(test)]
mod test {
    use crate::catalog::schema::Schema;
    use crate::hash_table::directory::Directory;
    use crate::page::{DiskObject, Page};

    #[test]
    fn test_depth_mask() {
        let mut dir = Directory::default();

        assert_eq!(dir.global_depth_mask(), 0);

        dir.set_global_depth(2);
        assert_eq!(dir.global_depth_mask(), 3);

        dir.set_global_depth(4);
        assert_eq!(dir.global_depth_mask(), 15);

        dir.set_global_depth(8);
        assert_eq!(dir.global_depth_mask(), 255);
    }

    #[test]
    fn test_directory() {
        let page = Page::default();
        let mut w = page.write();

        let mut dir = Directory::deserialise(w.data, &Schema::default());

        dir.insert(1, 1);
        dir.insert(2, 2);
        dir.insert(10, 10);

        assert_eq!(dir.get(1), 1);
        assert_eq!(dir.get(2), 2);
        assert_eq!(dir.get(10), 10);

        w.put2(&dir);

        // Make sure it reads back ok
        let dir = Directory::deserialise(w.data, &Schema::default());
        assert_eq!(dir.get(1), 1);
        assert_eq!(dir.get(2), 2);
        assert_eq!(dir.get(10), 10);
    }
}
