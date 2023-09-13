use std::{collections::BinaryHeap, mem::size_of};

use tokio::sync::RwLockWriteGuard;

use crate::{
    btree::{BTreeHeader, BTreeNodeType},
    get_bytes, get_i32,
    page::{PageId, PageInner, PAGE_SIZE},
    pair::Pair,
    put_bytes,
    storable::Storable,
    table_page::RelationID,
};

pub struct LeafNode<K> {
    pub header: BTreeHeader,
    next_page_id: PageId,
    pairs: BinaryHeap<Pair<K, RelationID>>,
}

impl<'a, K> LeafNode<K>
where
    K: Storable + Ord + 'a,
{
    pub fn new(data: &[u8; PAGE_SIZE]) -> Self {
        let header = BTreeHeader::new(data);
        let next_page_id = get_i32!(data, BTreeHeader::SIZE);

        let k_size = size_of::<K>();
        let v_size = size_of::<RelationID>();

        let mut pairs = BinaryHeap::new();
        let mut pos = BTreeHeader::SIZE;

        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_bytes.len();
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_bytes.len();

            // Check invalid rel id
            let rel_id = RelationID::from_bytes(v_bytes);
            if rel_id.0 == 0 || rel_id.1 == 0 {
                continue;
            }

            let key = K::from_bytes(k_bytes);

            pairs.push(Pair::new(key, rel_id));
        }

        Self {
            header,
            next_page_id,
            pairs,
        }
    }

    pub fn init(&mut self, size: u32, max_size: u32) {
        self.header.init(BTreeNodeType::Leaf, size, max_size);
    }

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, PageInner>) {
        self.header.write_data(&mut page.data);
        put_bytes!(
            page.data,
            self.next_page_id.to_be_bytes(),
            BTreeHeader::SIZE,
            size_of::<PageId>()
        );

        let mut pos = BTreeHeader::SIZE;
        let p_size = size_of::<K>() + size_of::<PageId>();
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

    pub fn insert(&mut self, k: K, rel_id: RelationID) {
        let pair = Pair::new(k, rel_id);

        self.pairs.push(pair);
    }
}
