use {
    crate::catalog::{Column, Schema, Type},
    bytes::{BufMut, BytesMut},
    std::{
        cmp::Ordering::{self, *},
        mem::size_of,
    },
};

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

    pub fn from(data: &[u8], column: &Column) -> Value {
        let data = match column.ty {
            Type::Varchar => {
                // First two bytes is the offset
                let offset =
                    u16::from_be_bytes(data[column.offset..column.offset + 2].try_into().unwrap())
                        as usize;

                // Second two bytes is the length
                let size = u16::from_be_bytes(
                    data[column.offset + 2..column.offset + 4].try_into().unwrap(),
                ) as usize;

                assert!(offset + size <= data.len());

                &data[offset..offset + size]
            }
            _ => {
                assert!(column.offset + column.value_size() <= data.len());
                &data[column.offset..column.offset + column.value_size()]
            }
        };

        match column.ty {
            Type::TinyInt => {
                assert_eq!(data.len(), size_of::<i8>());
                Value::TinyInt(i8::from_be_bytes(data.try_into().unwrap()))
            }
            Type::Bool => {
                assert_eq!(data.len(), size_of::<bool>());
                Value::Bool(u8::from_be_bytes(data.try_into().unwrap()) > 0)
            }
            Type::Int => {
                assert_eq!(data.len(), size_of::<i32>());
                Value::Int(i32::from_be_bytes(data.try_into().unwrap()))
            }
            Type::BigInt => {
                assert_eq!(data.len(), size_of::<i64>());
                Value::BigInt(i64::from_be_bytes(data.try_into().unwrap()))
            }
            Type::Varchar => {
                let str = std::str::from_utf8(data).expect("todo");
                Value::Varchar(str.into())
            }
        }
    }
}

// TODO: support NULL - include a null bitmap with each tuple if columns can be nullable
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Data(pub BytesMut);

// TODO: rewrite to use TupleBuilder
/// Given a buffer representing a tuple and a schema, reduce the buffer to match the schema
pub fn fit_tuple_with_schema(data: &[u8], schema: &Schema) -> Data {
    struct Variable<'a> {
        data: &'a [u8],
        offset_offset: usize,
    }

    let mut tuple = BytesMut::new();
    let mut vars = Vec::new();

    // `buf` could go extend beyond the tuple, use schema to read the correct amount of bytes
    // This assumes the tuple begins at the zeroth byte
    for Column { name: _, ty, offset } in &schema.columns {
        let start = tuple.len();
        tuple.put(&data[*offset..*offset + ty.size()]);

        if ty == &Type::Varchar {
            let (var_offset, length) = (
                u16::from_be_bytes((&data[*offset..*offset + 2]).try_into().unwrap()) as usize,
                u16::from_be_bytes((&data[*offset + 2..*offset + 4]).try_into().unwrap())
                    as usize,
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
        let value = self.get_value(&schema.columns[0]);

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
            builder = builder.add(&self.get_value(column));
        }

        builder.build()
    }

    pub fn get_value(&self, column: &Column) -> Value {
        Value::from(&self.0, column)
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
        for col in self.0.iter() {
            let lhs = Value::from(&self.1 .0, col);
            let rhs = Value::from(&other.1 .0, col);

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
    use {
        crate::{
            catalog::{Column, Schema, Type},
            table::tuple::{fit_tuple_with_schema, Builder, Comparand},
        },
        std::cmp::Ordering::*,
    };

    macro_rules! test_fit_tuple_with_schema {
        ($name:tt, schema: $schema:expr, tuple: $tuple:expr, want: $want:expr) => {
            #[test]
            fn $name() {
                let have = fit_tuple_with_schema(&$tuple.as_bytes(), &$schema.into());
                assert_eq!($want, have);
            }
        };
    }

    test_fit_tuple_with_schema! (
        fit_same_schema_2_columns,
        schema: [("col_b", Type::Varchar), ("col_c", Type::Int)],
        tuple: Builder::new().varchar("row_a").int(20).build(),
        want: Builder::new().varchar("row_a").int(20).build()
    );

    test_fit_tuple_with_schema! (
        fit_same_schema_3_columns,
        schema: [("col_a", Type::Int), ("col_b", Type::Varchar), ("col_c", Type::BigInt)],
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().int(10).varchar("row_a").big_int(20).build()
    );

    test_fit_tuple_with_schema! (
        fit_last_columns,
        schema: Schema::new(vec![
            Column { name: "col_b".into(), ty: Type::Varchar, offset: 4 },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 8 },
        ]),
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().varchar("row_a").big_int(20).build()
    );

    test_fit_tuple_with_schema! (
        fit_middle_column,
        schema: Schema::new(vec![Column {
            name: "col_b".into(),
            ty: Type::Varchar,
            offset: 4,
        }]),
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().varchar("row_a").build()
    );

    test_fit_tuple_with_schema! (
        fit_outer_columns,
        schema: Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 8 },
        ]),
        tuple: Builder::new().int(10).varchar("row_a").big_int(20).build(),
        want: Builder::new().int(10).big_int(20).build()
    );

    macro_rules! test_comparator {
        ($name:tt, $schema:expr, lhs: $lhs:expr, rhs: $rhs:expr, $want:expr) => {
            #[test]
            fn $name() {
                let have = Comparand(&$schema, &$lhs).cmp(&Comparand(&$schema, &$rhs));
                assert_eq!($want, have);
            }
        };
    }

    test_comparator!(
        t1,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
            Column { name: "col_b".into(), ty: Type::Bool, offset: 4 },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 5 },
        ]),
        lhs: Builder::new().int(4).bool(false).big_int(100).build(),
        rhs: Builder::new().int(4).bool(false).big_int(100).build(),
        Equal
    );

    test_comparator!(
        t2,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
            Column { name: "col_b".into(), ty: Type::Bool, offset: 4 },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 5 },
        ]),
        lhs: Builder::new().int(4).bool(true).big_int(100).build(),
        rhs: Builder::new().int(4).bool(false).big_int(100).build(),
        Greater
    );

    test_comparator!(
        t3,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
            Column { name: "col_b".into(), ty: Type::Bool, offset: 4 },
            Column { name: "col_c".into(), ty: Type::BigInt, offset: 5 },
        ]),
        lhs: Builder::new().int(4).bool(false).big_int(90).build(),
        rhs: Builder::new().int(4).bool(false).big_int(100).build(),
        Less
    );

    test_comparator!(
        t4,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::TinyInt, offset: 0 },
            Column { name: "col_b".into(), ty: Type::Varchar, offset: 1 },
        ]),
        lhs: Builder::new().tiny_int(1).varchar("Column").build(),
        rhs: Builder::new().tiny_int(1).varchar("Column").build(),
        Equal
    );

    test_comparator!(
        t5,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Varchar, offset: 0 },
            Column { name: "col_b".into(), ty: Type::TinyInt, offset: 4 },
        ]),
        lhs: Builder::new().varchar("Column A").tiny_int(1).build(),
        rhs: Builder::new().varchar("Column B").tiny_int(1).build(),
        Less
    );

    test_comparator!(
        t6,
        Schema::new(vec![
            Column { name: "col_a".into(), ty: Type::Varchar, offset: 0 },
            Column { name: "col_b".into(), ty: Type::TinyInt, offset: 4 },
        ]),
        lhs: Builder::new().varchar("Column A").tiny_int(1).build(),
        rhs: Builder::new().varchar("Column").tiny_int(1).build(),
        Greater
    );
}
