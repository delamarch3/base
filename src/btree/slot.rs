use std::mem::size_of;

use bytes::BytesMut;

use crate::{page::PageId, storable::Storable};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Either<V> {
    Value(V),
    Pointer(PageId),
}

#[macro_export]
macro_rules! get_ptr {
    ( $slot:ident ) => {
        match $slot.1 {
            Either::Value(_) => unreachable!(),
            Either::Pointer(ptr) => ptr,
        }
    };
}

#[macro_export]
macro_rules! get_value {
    ( $slot:ident ) => {
        match $slot.1 {
            Either::Value(value) => value,
            Either::Pointer(_) => unreachable!(),
        }
    };
}

impl<V> Either<V> {
    pub const SIZE: usize = 1 + size_of::<V>();
}

impl<V> From<&[u8]> for Either<V>
where
    V: Storable,
{
    fn from(value: &[u8]) -> Self {
        assert!(value.len() == Either::<V>::SIZE);

        let either = value[0];
        let value = &value[1..];
        match either {
            0 => {
                let value = V::from_bytes(value);
                Either::Value(value)
            }
            1 => {
                let b: [u8; 4] = value.try_into().unwrap();
                let ptr = i32::from_be_bytes(b);
                Either::Pointer(ptr)
            }
            _ => unreachable!(),
        }
    }
}

impl<V> From<&Either<V>> for BytesMut
where
    V: Storable,
{
    fn from(value: &Either<V>) -> Self {
        let mut ret = BytesMut::zeroed(Either::<V>::SIZE);
        match value {
            Either::Value(v) => {
                ret[0] = 0;
                v.write_to(&mut ret, 1);
            }
            Either::Pointer(p) => {
                ret[0] = 1;
                p.write_to(&mut ret, 1);
            }
        }

        ret
    }
}

pub trait Increment {
    fn increment(&mut self);
    fn next(&self) -> Self;
}

macro_rules! impl_increment {
    ($( $t:ty ),*) => {
        $(
        impl Increment for $t {
            fn increment(&mut self) {
                *self += 1;
            }

            fn next(&self) -> Self {
                self + 1
            }
        }
        )*
    };
}

impl_increment!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

// Size = 1 + size_of::<K>() + size_of::<V>()
// | Key | Flag (1) | Value
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct Slot<K, V>(pub K, pub Either<V>);

impl<K, V> Slot<K, V> {
    pub const SIZE: usize = size_of::<K>() + Either::<V>::SIZE;

    pub fn increment(&mut self)
    where
        K: Increment,
    {
        self.0.increment()
    }
}

impl<K, V> PartialOrd for Slot<K, V>
where
    K: Ord,
    V: PartialEq,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl<K, V> Ord for Slot<K, V>
where
    K: Ord,
    V: Eq,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<K, V> From<&[u8]> for Slot<K, V>
where
    K: Storable,
    V: Storable,
{
    fn from(value: &[u8]) -> Self {
        assert!(value.len() == Slot::<K, V>::SIZE);

        let ks = size_of::<K>();
        let key = K::from_bytes(&value[0..ks]);
        let value = Either::from(&value[ks..]);

        Self(key, value)
    }
}

impl<K, V> From<&Slot<K, V>> for BytesMut
where
    K: Storable,
    V: Storable,
{
    fn from(slot: &Slot<K, V>) -> Self {
        let mut ret = BytesMut::zeroed(Slot::<K, V>::SIZE);

        slot.0.write_to(&mut ret, 0);
        ret[size_of::<K>()..].copy_from_slice(&BytesMut::from(&slot.1));

        ret
    }
}
