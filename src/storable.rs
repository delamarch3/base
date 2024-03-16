use std::mem::size_of;

use bytes::BufMut;

use crate::page::PageId;

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
