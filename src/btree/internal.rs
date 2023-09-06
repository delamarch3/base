use std::{collections::BinaryHeap, mem::size_of};

use tokio::sync::RwLockWriteGuard;

use crate::{
    btree::{BTreeHeader, PageType},
    get_bytes,
    page::{Page, PageID, DEFAULT_PAGE_SIZE},
    pair::Pair,
    storable::Storable,
};

pub struct InternalNode<K, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    pub header: BTreeHeader,
    pairs: BinaryHeap<Pair<K, PageID>>,
}

impl<'a, const PAGE_SIZE: usize, K> InternalNode<K, PAGE_SIZE>
where
    K: Storable + Ord,
{
    pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
        let header = BTreeHeader::new(data);

        let k_size = size_of::<K>();
        let v_size = size_of::<PageID>();

        let mut pairs = BinaryHeap::new();
        let mut pos = BTreeHeader::SIZE;

        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_bytes.len();
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_bytes.len();

            // Check invalid page id
            let page_id = PageID::from_bytes(v_bytes);
            if page_id == 0 {
                continue;
            }

            let key = K::from_bytes(k_bytes);

            pairs.push(Pair::new(key, page_id));
        }

        Self { header, pairs }
    }

    pub fn init(&mut self, size: u32, max_size: u32) {
        self.header.init(PageType::Internal, size, max_size);
    }

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>) {
        self.header.write_data(&mut page.data);

        let mut pos = BTreeHeader::SIZE;
        let p_size = size_of::<K>() + size_of::<PageID>();
        for pair in &self.pairs {
            if pos + p_size >= PAGE_SIZE {
                break;
            }

            pair.a.write_to(&mut page.data, pos);
            pos += pair.a.len();
            pair.b.write_to(&mut page.data, pos);
            pos += pair.b.len();
        }

        page.dirty = true;
    }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn insert(&mut self, k: K, page_id: PageID) {
        self.pairs.push(Pair::new(k, page_id));
    }
}
