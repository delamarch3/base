use std::mem::size_of;

use bytes::{BufMut, BytesMut};

use crate::{byte_array, copy_bytes, get_u32, get_u64, put_bytes, table_page::RelationID};

#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub struct PairType<T>(pub T);

impl<T> PairType<T> {
    pub fn new(val: T) -> Self {
        Self(val)
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Eq)]
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

impl Into<BytesMut> for PairType<RelationID> {
    fn into(self) -> BytesMut {
        let mut ret = BytesMut::zeroed(size_of::<RelationID>());
        copy_bytes!(ret, u32::to_be_bytes(self.0 .0), 0, size_of::<u32>());
        copy_bytes!(
            ret,
            u64::to_be_bytes(self.0 .1),
            size_of::<u32>(),
            size_of::<u64>()
        );
        ret
    }
}

impl Into<PairType<RelationID>> for RelationID {
    fn into(self) -> PairType<RelationID> {
        PairType(self)
    }
}

impl From<&[u8]> for PairType<RelationID> {
    fn from(value: &[u8]) -> Self {
        let page_id = u32::from_be_bytes(byte_array!(u32, value));
        let slot_offset = u64::from_be_bytes(byte_array!(u64, value, size_of::<u32>()));
        PairType::new((page_id, slot_offset))
    }
}

impl<K> PartialOrd for Pair<K, RelationID>
where
    PairType<K>: Ord,
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl<K> Ord for Pair<K, RelationID>
where
    PairType<K>: Ord,
    K: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.a.cmp(&other.a)
    }
}

pub trait Storable {
    const SIZE: usize;
    type ByteArray;

    fn into_bytes(self) -> Self::ByteArray;
    fn from_bytes(bytes: &[u8]) -> Self;
    fn write_to(&self, dst: &mut [u8], pos: usize);

    fn len(&self) -> usize {
        Self::SIZE
    }
}

impl Storable for u32 {
    const SIZE: usize = 4;
    type ByteArray = [u8; Self::SIZE];

    fn into_bytes(self) -> [u8; Self::SIZE] {
        self.to_be_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        u32::from_be_bytes(bytes.try_into().unwrap())
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        put_bytes!(dst, self.into_bytes(), pos, Self::SIZE);
    }
}

impl Storable for RelationID {
    const SIZE: usize = 12;
    type ByteArray = [u8; Self::SIZE];

    fn into_bytes(self) -> Self::ByteArray {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        bytes.put_u32(self.0);
        bytes.put_u64(self.1);

        bytes.try_into().unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= Self::SIZE);

        let page_id = get_u32!(bytes, 0);
        let rel_id = get_u64!(bytes, 4);

        (page_id, rel_id)
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        put_bytes!(dst, self.into_bytes(), pos, Self::SIZE);
    }
}

#[derive(PartialEq, Eq)]
pub struct Pair2<A, B> {
    pub a: A,
    pub b: B,
}

impl<A, B> Pair2<A, B>
where
    A: Storable,
    B: Storable,
{
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

impl<K> PartialOrd for Pair2<K, RelationID>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl<K> Ord for Pair2<K, RelationID>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.a.cmp(&other.a)
    }
}
