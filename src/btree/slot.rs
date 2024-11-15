use std::mem::size_of;

use bytes::BytesMut;

use crate::{page::PageID, storable::Storable, table::tuple::Data as TupleData};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Either<V> {
    Value(V),
    Pointer(PageID),
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

// Size = 1 + Schema + size_of::<V>()
// | Key | Flag (1) | Value
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slot<V>(pub TupleData, pub Either<V>);

impl<V> From<&Slot<V>> for BytesMut
where
    V: Storable,
{
    fn from(Slot(TupleData(k), v): &Slot<V>) -> Self {
        let mut ret = BytesMut::zeroed(k.len() + Either::<V>::SIZE);

        ret[..k.len()].copy_from_slice(k);
        ret[k.len()..k.len() + Either::<V>::SIZE].copy_from_slice(&BytesMut::from(v));

        ret
    }
}

impl<V> Slot<V> {
    pub fn size(&self) -> usize {
        self.0.size() + Either::<V>::SIZE
    }
}
