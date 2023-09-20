use std::{collections::BinaryHeap, mem::size_of};

use tokio::sync::RwLockWriteGuard;

use crate::{
    btree::{BTreeHeader, BTreeNodeType},
    get_bytes,
    page::{PageId, PageInner, PAGE_SIZE},
    pair::Pair,
    storable::Storable,
};

pub struct InternalNode<K> {
    pub header: BTreeHeader,
    pub pairs: BinaryHeap<Pair<K, PageId>>,
}

impl<'a, K> InternalNode<K>
where
    K: Storable + Ord,
{
    pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
        let header = BTreeHeader::new(data);

        let k_size = size_of::<K>();
        let v_size = size_of::<PageId>();

        let mut pairs = BinaryHeap::new();
        let mut pos = BTreeHeader::SIZE;

        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_bytes.len();
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_bytes.len();

            // Check invalid page id
            let page_id = PageId::from_bytes(v_bytes);
            if page_id == 0 {
                continue;
            }

            let key = K::from_bytes(k_bytes);

            pairs.push(Pair::new(key, page_id));
        }

        Self { header, pairs }
    }

    pub fn init(&mut self, size: u32, max_size: u32) {
        self.header.init(BTreeNodeType::Internal, size, max_size);
    }

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, PageInner>) {
        self.header.write_data(&mut page.data);

        let mut pos = BTreeHeader::SIZE;
        let p_size = size_of::<K>() + size_of::<PageId>();
        for pair in &self.pairs {
            if pos + p_size >= PAGE_SIZE {
                break;
            }

            pair.a.write_to(&mut page.data, pos);
            pos += pair.a.size();
            pair.b.write_to(&mut page.data, pos);
            pos += pair.b.size();
        }

        page.dirty = true;
    }

    pub fn insert(&mut self, k: K, page_id: PageId) {
        self.pairs.push(Pair::new(k, page_id));
        self.header.len += 1;
    }

    pub fn r#type(&self) -> BTreeNodeType {
        self.header.r#type()
    }
}
