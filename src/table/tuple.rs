use crate::catalog::schema::{Column, Schema, Type};
use bytes::{BufMut, BytesMut};
use std::cmp::Ordering::{self, *};
use std::mem::size_of;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Value {
    TinyInt(i8),
    Bool(bool),
    Int(i32),
    BigInt(i64),
    Varchar(String),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::TinyInt(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int(v) => write!(f, "{}", v),
            Value::BigInt(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "\"{}\"", v),
        }
    }
}

impl Value {
    pub fn ty(&self) -> Type {
        match self {
            Value::TinyInt(_) => Type::TinyInt,
            Value::Bool(_) => Type::Bool,
            Value::Int(_) => Type::Int,
            Value::BigInt(_) => Type::BigInt,
            Value::Varchar(_) => Type::Varchar,
        }
    }
}

// TODO: support NULL - include a null bitmap with each tuple if columns can be nullable
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Data(pub BytesMut);

/// Creates a new tuple using only the columns in the schema. The schema offsets are expected to
/// align with the offsets in the tuple. This is useful for creating composite key tuple out of
/// non-contiguous columns.
pub fn fit_tuple_with_schema(tuple: &Data, schema: &Schema) -> Data {
    let mut builder = Builder::new();
    for Column { offset, ty, .. } in &schema.columns {
        let value = tuple.get_value(*offset, *ty);
        builder = builder.add(&value);
    }

    builder.build()
}

/// Creates a new tuple using only the columns in the schema. This is useful for unmarshalling
/// tuples out of page objects.
pub fn bytes_to_tuple(data: &[u8], schema: &Schema) -> Data {
    struct Variable<'a> {
        data: &'a [u8],
        offset_offset: usize,
    }

    let mut tuple = BytesMut::new();
    let mut vars = Vec::new();

    // `buf` could go extend beyond the tuple, use schema to read the correct amount of bytes
    // This assumes the tuple begins at the zeroth byte
    for Column { name: _, ty, offset, table: _ } in &schema.columns {
        let start = tuple.len();
        tuple.put(&data[*offset..*offset + ty.size()]);

        if ty == &Type::Varchar {
            let (var_offset, length) = (
                u16::from_be_bytes((&data[*offset..*offset + 2]).try_into().unwrap()) as usize,
                u16::from_be_bytes((&data[*offset + 2..*offset + 4]).try_into().unwrap()) as usize,
            );

            // Data to add on at the end of the tuple
            vars.push(Variable {
                data: &data[var_offset..var_offset + length],
                offset_offset: start,
            });
        }
    }

    // Variable length section
    for Variable { data, offset_offset } in vars {
        // Write correct offset
        let offset = tuple.len();
        tuple[offset_offset..offset_offset + 2].copy_from_slice(&u16::to_be_bytes(offset as u16));
        tuple.put(data);
    }

    Data(tuple)
}

impl Data {
    pub fn new(data: &[u8]) -> Self {
        Self(BytesMut::from(data))
    }

    pub fn empty() -> Self {
        Self(BytesMut::new())
    }

    /// Increments the value of the first value of the tuple
    pub fn increment(&mut self, schema: &Schema) {
        *self = self.next(schema);
    }

    /// Gets the next tuple, which has the value of the first column incremented by 1. This returns
    /// a new tuple, use `increment` to modify the current tuple.
    /// For integers it's a simple increment (TODO: handle overflow)
    /// Bool will always be set to true
    /// Varchars will have their first char incremented by 1
    pub fn next(&self, schema: &Schema) -> Self {
        assert!(schema.len() > 0);

        // Increment the first column
        let value = self.get_value(schema.columns[0].offset, schema.columns[0].ty);

        let value = match value {
            Value::TinyInt(v) => Value::TinyInt(v + 1),
            Value::Bool(_) => Value::Bool(true),
            Value::Int(v) => Value::Int(v + 1),
            Value::BigInt(v) => Value::BigInt(v + 1),
            Value::Varchar(mut v) => {
                if let Some(c) = v.chars().nth(0) {
                    let next = char::from_u32(c as u32 + 1).expect("invalid char");
                    let len = char::len_utf8(next);
                    let buf = unsafe { v.as_bytes_mut() };
                    buf[0..len].copy_from_slice(&u32::to_be_bytes(next as u32)[0..len]);

                    Value::Varchar(v)
                } else {
                    Value::Varchar("\0".into())
                }
            }
        };

        let mut builder = Builder::new().add(&value);
        for column in &schema.columns[1..] {
            builder = builder.add(&self.get_value(column.offset, column.ty));
        }

        builder.build()
    }

    // pub fn get_value(&self, column: &Column) -> Value {
    pub fn get_value(&self, offset: usize, ty: Type) -> Value {
        let Self(buf) = self;

        let cell = match ty {
            Type::Varchar => {
                // First two bytes is the offset
                let var_offset =
                    u16::from_be_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;

                // Second two bytes is the length
                let size =
                    u16::from_be_bytes(buf[offset + 2..offset + 4].try_into().unwrap()) as usize;

                assert!(var_offset + size <= buf.len());

                &buf[var_offset..var_offset + size]
            }
            _ => {
                assert!(offset + ty.size() <= buf.len());
                &buf[offset..offset + ty.size()]
            }
        };

        match ty {
            Type::TinyInt => {
                assert_eq!(cell.len(), size_of::<i8>());
                Value::TinyInt(i8::from_be_bytes(cell.try_into().unwrap()))
            }
            Type::Bool => {
                assert_eq!(cell.len(), size_of::<bool>());
                Value::Bool(u8::from_be_bytes(cell.try_into().unwrap()) > 0)
            }
            Type::Int => {
                assert_eq!(cell.len(), size_of::<i32>());
                Value::Int(i32::from_be_bytes(cell.try_into().unwrap()))
            }
            Type::BigInt => {
                assert_eq!(cell.len(), size_of::<i64>());
                Value::BigInt(i64::from_be_bytes(cell.try_into().unwrap()))
            }
            Type::Varchar => {
                let str = std::str::from_utf8(cell).expect("todo");
                Value::Varchar(str.into())
            }
        }
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

pub struct Comparand<'a, T>(pub &'a Schema, pub T);

impl<'a, 'b> PartialEq for Comparand<'a, &'b Data> {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(other.1)
    }
}
impl<'a, 'b> Eq for Comparand<'a, &'b Data> {}

impl<'a, 'b> PartialOrd for Comparand<'a, &'b Data> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, 'b> Ord for Comparand<'a, &'b Data> {
    fn cmp(&self, other: &Self) -> Ordering {
        for column in self.0.iter() {
            let lhs = self.1.get_value(column.offset, column.ty);
            let rhs = other.1.get_value(column.offset, column.ty);

            match lhs.cmp(&rhs) {
                Less => return Less,
                Greater => return Greater,
                _ => {}
            }
        }

        Equal
    }
}

impl<'a> PartialEq for Comparand<'a, i32> {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}
impl<'a> Eq for Comparand<'a, i32> {}

impl<'a> PartialOrd for Comparand<'a, i32> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Comparand<'a, i32> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1)
    }
}

impl From<i32> for Data {
    fn from(val: i32) -> Self {
        Builder::new().int(val).build()
    }
}

struct Variable {
    data: BytesMut,
    offset_offset: usize,
}

#[derive(Default)]
pub struct Builder {
    data: BytesMut,
    variable: Vec<Variable>,
}

impl Builder {
    pub fn new() -> Self {
        Self { data: BytesMut::new(), ..Default::default() }
    }

    pub fn with_capacity(size: usize) -> Self {
        Self { data: BytesMut::with_capacity(size), ..Default::default() }
    }

    pub fn add(self, value: &Value) -> Self {
        match value {
            Value::TinyInt(v) => self.tiny_int(*v),
            Value::Bool(v) => self.bool(*v),
            Value::Int(v) => self.int(*v),
            Value::BigInt(v) => self.big_int(*v),
            Value::Varchar(v) => self.varchar(v),
        }
    }

    pub fn tiny_int(mut self, value: i8) -> Self {
        self.data.put(&value.to_be_bytes()[..]);
        self
    }

    pub fn bool(mut self, value: bool) -> Self {
        self.data.put(&u8::to_be_bytes(if value { 1 } else { 0 })[..]);
        self
    }

    pub fn int(mut self, value: i32) -> Self {
        self.data.put(&value.to_be_bytes()[..]);
        self
    }

    pub fn big_int(mut self, value: i64) -> Self {
        self.data.put(&value.to_be_bytes()[..]);
        self
    }

    pub fn varchar(mut self, value: &str) -> Self {
        let offset = self.data.len();

        // First two bytes is the offset, which we won't know until build()
        // Second two bytes is the length
        self.data.resize(offset + 4, 0);
        self.data[offset + 2..offset + 4].copy_from_slice(&u16::to_be_bytes(value.len() as u16));

        self.variable.push(Variable { data: BytesMut::from(value), offset_offset: offset });

        self
    }

    pub fn build(mut self) -> Data {
        for Variable { data, offset_offset } in self.variable {
            let offset = self.data.len();

            // Update offset
            self.data[offset_offset..offset_offset + 2]
                .copy_from_slice(&u16::to_be_bytes(offset as u16));

            // Write variable length data to end of tuple
            self.data.put(data);
        }

        Data(self.data)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::schema::{Column, Schema, Type};
    use crate::table::tuple::{fit_tuple_with_schema, Builder, Comparand};
    use crate::{column, schema};

    use std::cmp::Ordering::*;

    macro_rules! test_fit_tuple_with_schema {
        ($name:tt, $schema:expr, tuple: $tuple:expr, want: $want:expr) => {
            #[test]
            fn $name() {
                let have = fit_tuple_with_schema(&$tuple, &$schema.into());
                assert_eq!($want, have);
            }
        };
    }

    test_fit_tuple_with_schema! (
        fit_same_schema_2_columns,
        schema! {column!("col_b", Varchar), column!("col_c", Int)},
        tuple: Builder::new().varchar("row_a").int(20).build(),
        want: Builder::new().varchar("row_a").int(20).build()
    );

    test_fit_tuple_with_schema! (
        fit_same_schema_3_columns,
        schema! {column!("col_a", Int), column!("col_b", Varchar), column!("col_c", BigInt)},
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().int(10).varchar("row_a").big_int(20).build()
    );

    test_fit_tuple_with_schema! (
        fit_last_columns,
        Schema::new(vec![
            Column { name: "col_b".into(), ty: Type::Varchar, offset: 4, table: None },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 8, table: None },
        ]),
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().varchar("row_a").big_int(20).build()
    );

    test_fit_tuple_with_schema! (
        fit_middle_column,
        Schema::new(vec![Column {
            name: "col_b".into(),
            ty: Type::Varchar,
            offset: 4,
            table: None
        }]),
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().varchar("row_a").build()
    );

    test_fit_tuple_with_schema! (
        fit_outer_columns,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Int, offset: 0, table: None },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 8, table: None },
        ]),
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().int(10).big_int(20).build()
    );

    macro_rules! test_comparator {
        ($name:tt, $schema:expr, lhs: $lhs:expr, rhs: $rhs:expr, $want:expr) => {
            #[test]
            fn $name() {
                let schema = $schema;
                let have = Comparand(&schema, &$lhs).cmp(&Comparand(&schema, &$rhs));
                assert_eq!($want, have);
            }
        };
    }

    test_comparator!(
        t1,
        schema! {column!("c1", Int), column!("c2", Bool), column!("c1", BigInt)},
        lhs: Builder::new().int(4).bool(false).big_int(100).build(),
        rhs: Builder::new().int(4).bool(false).big_int(100).build(),
        Equal
    );

    test_comparator!(
        t2,
        schema! {column!("c1", Int), column!("c2", Bool), column!("c1", BigInt)},
        lhs: Builder::new().int(4).bool(true).big_int(100).build(),
        rhs: Builder::new().int(4).bool(false).big_int(100).build(),
        Greater
    );

    test_comparator!(
        t3,
        schema! {column!("c1", Int), column!("c2", Bool), column!("c1", BigInt)},
        lhs: Builder::new().int(4).bool(false).big_int(90).build(),
        rhs: Builder::new().int(4).bool(false).big_int(100).build(),
        Less
    );

    test_comparator!(
        t4,
        schema! {column!("c1", TinyInt), column!("c2", Varchar)},
        lhs: Builder::new().tiny_int(1).varchar("Column").build(),
        rhs: Builder::new().tiny_int(1).varchar("Column").build(),
        Equal
    );

    test_comparator!(
        t5,
        schema! {column!("c1", Varchar), column!("c2", TinyInt)},
        lhs: Builder::new().varchar("Column A").tiny_int(1).build(),
        rhs: Builder::new().varchar("Column B").tiny_int(1).build(),
        Less
    );

    test_comparator!(
        t6,
        schema! {column!("c1", Varchar), column!("c2", TinyInt)},
        lhs: Builder::new().varchar("Column A").tiny_int(1).build(),
        rhs: Builder::new().varchar("Column").tiny_int(1).build(),
        Greater
    );
}
