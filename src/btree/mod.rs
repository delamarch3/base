use std::mem::size_of;

use crate::{get_u32, put_bytes};

pub mod btree;
pub mod internal;
pub mod leaf;

#[derive(PartialEq, Clone, Copy)]
pub enum PageType {
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

pub struct BTreeHeader {
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

    pub fn init(&mut self, t: PageType, size: u32, max_size: u32) {
        self.t = t;
        self.size = size;
        self.max_size = max_size;
    }
}
