use std::{
    cmp::Ordering::{self, *},
    ops::Range,
};

use bytes::BytesMut;

use crate::{
    btree::slot::Increment,
    catalog::{Column, Schema, Value},
    page::PageId,
    storable::Storable,
};

#[derive(Debug, PartialEq, Copy, Clone, PartialOrd, Eq, Ord)]
pub struct RId {
    pub page_id: PageId,
    pub slot_id: u32,
}

// TODO
impl Storable for RId {
    const SIZE: usize = 0;

    type ByteArray = [u8; 0];

    fn into_bytes(self) -> Self::ByteArray {
        todo!()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        todo!()
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        todo!()
    }
}

#[derive(Debug, PartialEq, Clone, PartialOrd, Eq, Ord)]
pub struct Tuple {
    pub rid: RId,
    pub data: BytesMut,
}

impl Increment for Tuple {
    fn increment(&mut self) {
        todo!()
    }

    fn next(&self) -> Self {
        todo!()
    }
}

// TODO
impl Storable for Tuple {
    const SIZE: usize = 0;

    type ByteArray = [u8; 0];

    fn into_bytes(self) -> Self::ByteArray {
        todo!()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        todo!()
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        todo!()
    }
}

impl Tuple {
    pub fn get_value(&self, column: &Column) -> Value {
        Value::from(&column, &self.data)
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct TupleMeta {
    pub deleted: bool,
}

impl From<&[u8]> for TupleMeta {
    fn from(value: &[u8]) -> Self {
        let deleted = u8::from_be_bytes(value[0..1].try_into().unwrap()) > 1;

        Self { deleted }
    }
}

pub const OFFSET: Range<usize> = 0..4;
pub const LEN: Range<usize> = 4..8;
pub const META: Range<usize> = 8..Slot::SIZE;

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Slot {
    pub offset: u32,
    pub len: u32,
    pub meta: TupleMeta,
}

impl From<&[u8]> for Slot {
    fn from(buf: &[u8]) -> Self {
        let offset = u32::from_be_bytes(buf[OFFSET].try_into().unwrap());
        let len = u32::from_be_bytes(buf[LEN].try_into().unwrap());
        let meta = TupleMeta::from(&buf[META]);

        Self { offset, len, meta }
    }
}

impl Slot {
    pub const SIZE: usize = 9;
}

pub type TupleInfoBuf = [u8; Slot::SIZE];
impl From<&Slot> for TupleInfoBuf {
    fn from(value: &Slot) -> Self {
        let mut ret = [0; Slot::SIZE];

        ret[OFFSET].copy_from_slice(&value.offset.to_be_bytes());
        ret[LEN].copy_from_slice(&value.len.to_be_bytes());
        ret[META].copy_from_slice(&[value.meta.deleted as u8]);

        ret
    }
}

pub struct Comparand<'a, 'b>(&'a Schema, &'b Tuple);

impl<'a, 'b> PartialEq for Comparand<'a, 'b> {
    fn eq(&self, other: &Self) -> bool {
        self.1.data.eq(&other.1.data)
    }
}

impl<'a, 'b> Eq for Comparand<'a, 'b> {}

impl<'a, 'b> PartialOrd for Comparand<'a, 'b> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, 'b> Ord for Comparand<'a, 'b> {
    fn cmp(&self, other: &Self) -> Ordering {
        for (_, col) in self.0.iter().enumerate() {
            let lhs = self.1.get_value(col);
            let rhs = other.1.get_value(col);

            match lhs.cmp(&rhs) {
                Less => return Less,
                Greater => return Greater,
                _ => {}
            }
        }

        Equal
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use std::{
        cmp::Ordering::{self, *},
        mem::size_of,
    };

    use crate::{
        catalog::{Column, Schema, Type, Value},
        page::PAGE_SIZE,
        table::tuple::{Comparand, RId, Tuple},
    };

    fn into_bytes(values: Vec<Value>) -> BytesMut {
        let mut bytes = BytesMut::zeroed(PAGE_SIZE);

        let mut offset = 0;
        for v in values {
            match v {
                Value::TinyInt(v) => {
                    bytes[offset..offset + size_of::<i8>()].copy_from_slice(&i8::to_be_bytes(v));
                    offset += size_of::<i8>();
                }
                Value::Bool(v) => {
                    bytes[offset..offset + size_of::<bool>()]
                        .copy_from_slice(&u8::to_be_bytes(if v { 1 } else { 0 }));
                    offset += size_of::<bool>();
                }
                Value::Int(v) => {
                    bytes[offset..offset + size_of::<i32>()].copy_from_slice(&i32::to_be_bytes(v));
                    offset += size_of::<i32>();
                }
                Value::BigInt(v) => {
                    bytes[offset..offset + size_of::<i64>()].copy_from_slice(&i64::to_be_bytes(v));
                    offset += size_of::<i64>();
                }
                Value::Varchar(v) => {
                    // TODO: Save space
                    bytes[offset..offset + 2].copy_from_slice(&u16::to_be_bytes(v.len() as u16));
                    bytes[offset + 2..offset + 2 + v.len()].copy_from_slice(v.as_bytes());
                    offset += 255 + 2;
                }
            }
        }

        bytes
    }

    #[test]
    fn test_comparator() {
        let rid = RId {
            page_id: 0,
            slot_id: 0,
        };

        struct Test {
            schema: Schema,
            lhs: Vec<Value>,
            rhs: Vec<Value>,
            want: Ordering,
        }

        let tcs = [
            Test {
                schema: Schema::new(vec![
                    Column {
                        name: "col_a".into(),
                        ty: Type::Int,
                        offset: 0,
                    },
                    Column {
                        name: "col_b".into(),
                        ty: Type::Bool,
                        offset: 4,
                    },
                    Column {
                        name: "col_c".into(),
                        ty: Type::BigInt,
                        offset: 5,
                    },
                ]),
                lhs: vec![Value::Int(4), Value::Bool(false), Value::BigInt(100)],
                rhs: vec![Value::Int(4), Value::Bool(false), Value::BigInt(100)],
                want: Equal,
            },
            Test {
                schema: Schema::new(vec![
                    Column {
                        name: "col_a".into(),
                        ty: Type::Int,
                        offset: 0,
                    },
                    Column {
                        name: "col_b".into(),
                        ty: Type::Bool,
                        offset: 4,
                    },
                    Column {
                        name: "col_c".into(),
                        ty: Type::BigInt,
                        offset: 5,
                    },
                ]),
                lhs: vec![Value::Int(4), Value::Bool(true), Value::BigInt(100)],
                rhs: vec![Value::Int(4), Value::Bool(false), Value::BigInt(100)],
                want: Greater,
            },
            Test {
                schema: Schema::new(vec![
                    Column {
                        name: "col_a".into(),
                        ty: Type::Int,
                        offset: 0,
                    },
                    Column {
                        name: "col_b".into(),
                        ty: Type::Bool,
                        offset: 4,
                    },
                    Column {
                        name: "col_c".into(),
                        ty: Type::BigInt,
                        offset: 5,
                    },
                ]),
                lhs: vec![Value::Int(4), Value::Bool(false), Value::BigInt(90)],
                rhs: vec![Value::Int(4), Value::Bool(false), Value::BigInt(100)],
                want: Less,
            },
            Test {
                schema: Schema::new(vec![
                    Column {
                        name: "col_a".into(),
                        ty: Type::TinyInt,
                        offset: 0,
                    },
                    Column {
                        name: "col_b".into(),
                        ty: Type::Varchar,
                        offset: 1,
                    },
                ]),
                lhs: vec![Value::TinyInt(1), Value::Varchar("Column".into())],
                rhs: vec![Value::TinyInt(1), Value::Varchar("Column".into())],
                want: Equal,
            },
            Test {
                schema: Schema::new(vec![
                    Column {
                        name: "col_a".into(),
                        ty: Type::Varchar,
                        offset: 0,
                    },
                    Column {
                        name: "col_b".into(),
                        ty: Type::TinyInt,
                        offset: 255 + 2,
                    },
                ]),
                lhs: vec![Value::Varchar("Column A".into()), Value::TinyInt(1)],
                rhs: vec![Value::Varchar("Column B".into()), Value::TinyInt(1)],
                want: Less,
            },
            Test {
                schema: Schema::new(vec![
                    Column {
                        name: "col_a".into(),
                        ty: Type::Varchar,
                        offset: 0,
                    },
                    Column {
                        name: "col_b".into(),
                        ty: Type::TinyInt,
                        offset: 255 + 2,
                    },
                ]),
                lhs: vec![Value::Varchar("Column A".into()), Value::TinyInt(1)],
                rhs: vec![Value::Varchar("Column".into()), Value::TinyInt(1)],
                want: Greater,
            },
        ];

        for Test {
            schema,
            lhs,
            rhs,
            want,
        } in tcs
        {
            let lhs = Tuple {
                rid,
                data: into_bytes(lhs),
            };
            let rhs = Tuple {
                rid,
                data: into_bytes(rhs),
            };

            let have = Comparand(&schema, &lhs).cmp(&Comparand(&schema, &rhs));
            assert_eq!(want, have);
        }
    }
}
