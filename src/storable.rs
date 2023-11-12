use std::mem::size_of;

use bytes::BufMut;

use crate::page::PageId;

// TODO: move this to correct file once time comes
type RelationID = (PageId, u64);

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
                dst[pos..pos + Self::SIZE].copy_from_slice(&self.into_bytes());
            }
        }
        )*
    };
}

storable_impl!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

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

        let page_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let rel_id = u64::from_be_bytes(bytes[4..12].try_into().unwrap());

        (page_id, rel_id)
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        dst[pos..pos + Self::SIZE].copy_from_slice(&self.into_bytes());
    }
}
