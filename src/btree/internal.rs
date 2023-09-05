use std::mem::size_of;

use bytes::BytesMut;
use tokio::sync::RwLockWriteGuard;

use crate::{
    btree::BTreeHeader,
    get_bytes,
    page::{Page, PageID, DEFAULT_PAGE_SIZE},
    pair::{Pair, PairType},
    put_bytes,
};

pub struct InternalNode<K, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    header: BTreeHeader,
    pairs: Vec<Pair<K, PageID>>,
}

impl<'a, const PAGE_SIZE: usize, K> InternalNode<K, PAGE_SIZE>
where
    PairType<K>: Into<BytesMut> + From<&'a [u8]> + Copy,
{
    pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
        let header = BTreeHeader::new(data);

        let k_size = size_of::<K>();
        let v_size = size_of::<PageID>();

        let mut pairs = Vec::new();
        let mut pos = BTreeHeader::SIZE;

        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_bytes.len();
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_bytes.len();

            // Check invalid page id
            let page_id: PairType<PageID> = v_bytes.into();
            if page_id == 0 {
                continue;
            }
            let key: PairType<K> = k_bytes.into();

            // Pair::from_bytes funny behaviour here
            pairs.push(Pair { a: key, b: page_id });
        }

        Self { header, pairs }
    }

    pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>) {
        self.header.write_data(&mut page.data);

        let mut pos = BTreeHeader::SIZE;
        let p_size = size_of::<K>() + size_of::<PageID>();
        for pair in &self.pairs {
            if pos + p_size >= PAGE_SIZE {
                break;
            }

            let key: BytesMut = pair.a.into();
            let value: BytesMut = pair.b.into();

            put_bytes!(page.data, key, pos, key.len());
            pos += key.len();
            put_bytes!(page.data, value, pos, value.len());
            pos += value.len();
        }

        page.dirty = true;
    }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }
}
