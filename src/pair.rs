use std::mem::size_of;

use bytes::BytesMut;

use crate::{byte_array, copy_bytes};

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct PairType<T>(T);

impl<T> PairType<T> {
    pub fn new(val: T) -> Self {
        Self(val)
    }
}

#[derive(Debug, PartialEq)]
pub struct Pair<A, B> {
    pub a: PairType<A>,
    pub b: PairType<B>,
}

impl<'a, A, B> Pair<A, B>
where
    PairType<A>: From<&'a [u8]>,
    PairType<B>: From<&'a [u8]>,
{
    pub fn new(a: A, b: B) -> Self {
        Self {
            a: PairType::new(a),
            b: PairType::new(b),
        }
    }

    pub fn from_bytes(a: &'a [u8], b: &'a [u8]) -> Self {
        Self {
            a: a.into(),
            b: b.into(),
        }
    }
}

impl<A, B> PartialEq<(A, B)> for Pair<A, B>
where
    A: PartialEq,
    B: PartialEq,
{
    fn eq(&self, other: &(A, B)) -> bool {
        self.a.0 == other.0 && self.b.0 == other.1
    }
}

impl<T> PartialEq<T> for PairType<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        *other == self.0
    }
}

macro_rules! pair_impl {
    ($( $t:ty ),*) => {
        $(
            impl Into<BytesMut> for PairType<$t> {
                fn into(self) -> BytesMut {
                    let mut ret = BytesMut::zeroed(size_of::<$t>());
                    copy_bytes!(ret, <$t>::to_be_bytes(self.0), 0, size_of::<$t>());
                    ret
                }
            }

            impl Into<PairType<$t>> for $t {
                fn into(self) -> PairType<$t> {
                    PairType(self)
                }
            }

            impl From<&[u8]> for PairType<$t> {
                fn from(value: &[u8]) -> Self {
                    PairType::new(<$t>::from_be_bytes(byte_array!($t, value)))
                }
            }
        )*
    };
}

pair_impl!(u8, u16, u32, u64, i8, i16, i32, i64);
