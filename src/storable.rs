use std::mem::size_of;

use bytes::BufMut;

use crate::{get_i32, get_u64, put_bytes, table_page::RelationID};

pub trait Storable: std::fmt::Debug {
    const SIZE: usize;
    type ByteArray;

    fn into_bytes(self) -> Self::ByteArray;
    fn from_bytes(bytes: &[u8]) -> Self;
    fn write_to(&self, dst: &mut [u8], pos: usize);

    fn size(&self) -> usize {
        Self::SIZE
    }
}

macro_rules! storable_impl {
    ($( $t:ty ),*) => {
        $(
        impl Storable for $t {
            const SIZE: usize = size_of::<$t>();
            type ByteArray = [u8; Self::SIZE];

            fn into_bytes(self) -> [u8; Self::SIZE] {
                self.to_be_bytes()
            }

            fn from_bytes(bytes: &[u8]) -> Self {
                <$t>::from_be_bytes(bytes.try_into().unwrap())
            }

            fn write_to(&self, dst: &mut [u8], pos: usize) {
                put_bytes!(dst, self.into_bytes(), pos, Self::SIZE);
            }
        }
        )*
    };
}

storable_impl!(u8, u16, u32, u64, i8, i16, i32, i64);

impl Storable for RelationID {
    const SIZE: usize = 12;
    type ByteArray = [u8; Self::SIZE];

    fn into_bytes(self) -> Self::ByteArray {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        bytes.put_i32(self.0);
        bytes.put_u64(self.1);

        bytes.try_into().unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= Self::SIZE);

        let page_id = get_i32!(bytes, 0);
        let rel_id = get_u64!(bytes, 4);

        (page_id, rel_id)
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        put_bytes!(dst, self.into_bytes(), pos, Self::SIZE);
    }
}
