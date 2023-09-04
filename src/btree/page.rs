use std::mem::size_of;

use bytes::BytesMut;
use tokio::sync::RwLockWriteGuard;

use crate::{
    get_bytes, get_u32,
    page::{Page, PageID, DEFAULT_PAGE_SIZE},
    pair::{Pair, PairType},
    put_bytes,
    table_page::RelationID,
};

#[derive(PartialEq, Clone, Copy)]
enum PageType {
    Invalid,
    Internal,
    Leaf,
}

impl Into<u32> for PageType {
    fn into(self) -> u32 {
        match self {
            PageType::Invalid => 0,
            PageType::Internal => 1,
            PageType::Leaf => 2,
        }
    }
}

impl Into<PageType> for u32 {
    fn into(self) -> PageType {
        match self {
            1 => PageType::Internal,
            2 => PageType::Leaf,
            _ => PageType::Invalid,
        }
    }
}

struct BTreeHeader {
    t: PageType,
    size: u32,
    max_size: u32,
}

impl BTreeHeader {
    const SIZE: usize = size_of::<u32>() * 3;

    pub fn new(data: &[u8]) -> Self {
        let t = get_u32!(data, 0);
        let size = get_u32!(data, 4);
        let max_size = get_u32!(data, 8);

        Self {
            t: t.into(),
            size,
            max_size,
        }
    }

    pub fn write_data(&self, page: &mut [u8]) {
        put_bytes!(page, Into::<u32>::into(self.t).to_be_bytes(), 0, 4);
        put_bytes!(page, self.size.to_be_bytes(), 4, 8);
        put_bytes!(page, self.max_size.to_be_bytes(), 8, 12);
    }

    pub fn set_page_type(&mut self, t: PageType) {
        self.t = t;
    }

    pub fn is_leaf(&self) -> bool {
        self.t == PageType::Leaf
    }
}

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

        // Page ID = 0 => invalid

        let mut pairs = Vec::new();
        let mut pos = BTreeHeader::SIZE;

        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_bytes.len();
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_bytes.len();

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
    }
}

pub struct LeafNode<K> {
    header: BTreeHeader,
    next_page_id: PageID,
    pairs: Vec<Pair<K, RelationID>>,
}
