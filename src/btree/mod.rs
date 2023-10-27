use std::mem::size_of;

use crate::{get_u32, put_bytes};

pub mod btree;
pub mod internal;
pub mod leaf;

#[derive(PartialEq, Clone, Copy)]
pub enum BTreeNodeType {
    Invalid,
    Internal,
    Leaf,
}

impl From<BTreeNodeType> for u32 {
    fn from(value: BTreeNodeType) -> Self {
        match value {
            BTreeNodeType::Invalid => 0,
            BTreeNodeType::Internal => 1,
            BTreeNodeType::Leaf => 2,
        }
    }
}

impl From<u32> for BTreeNodeType {
    fn from(value: u32) -> Self {
        match value {
            1 => BTreeNodeType::Internal,
            2 => BTreeNodeType::Leaf,
            _ => BTreeNodeType::Invalid,
        }
    }
}

pub struct BTreeHeader {
    t: BTreeNodeType,
    len: u32,
    max_len: u32,
}

impl BTreeHeader {
    const SIZE: usize = size_of::<u32>() * 3;

    pub fn new(data: &[u8]) -> Self {
        let t = get_u32!(data, 0);
        let size = get_u32!(data, 4);
        let max_size = get_u32!(data, 8);

        Self {
            t: t.into(),
            len: size,
            max_len: max_size,
        }
    }

    pub fn write_data(&self, page: &mut [u8]) {
        let t: u32 = self.t.into();
        put_bytes!(page, t.to_be_bytes(), 0, 4);
        put_bytes!(page, self.len.to_be_bytes(), 4, 8);
        put_bytes!(page, self.max_len.to_be_bytes(), 8, 12);
    }

    pub fn init(&mut self, t: BTreeNodeType, len: u32, max_len: u32) {
        self.t = t;
        self.len = len;
        self.max_len = max_len;
    }

    pub fn r#type(&self) -> BTreeNodeType {
        self.t
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    // TODO: this isn't the correct criteria for "full"
    pub fn almost_full(&self) -> bool {
        self.len + 1 == self.max_len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
