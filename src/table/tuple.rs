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

impl From<&Value> for Type {
    fn from(value: &Value) -> Self {
        match value {
            Value::TinyInt(_) => Type::TinyInt,
            Value::Bool(_) => Type::Bool,
            Value::Int(_) => Type::Int,
            Value::BigInt(_) => Type::BigInt,
            Value::Varchar(_) => Type::Varchar,
        }
    }
}

impl Value {
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

        match ty {
            Type::Varchar => {
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
            _ => {}
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
        Value::from(&self.0, &column)
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
        self.1.eq(&other.1)
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
        for (_, col) in self.0.iter().enumerate() {
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

impl Into<Data> for i32 {
    fn into(self) -> Data {
        Builder::new().add(&Value::Int(self)).build()
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

    pub fn add(mut self, value: &Value) -> Self {
        match value {
            Value::TinyInt(v) => self.data.put(&i8::to_be_bytes(*v)[..]),
            Value::Bool(v) => self.data.put(&u8::to_be_bytes(if *v { 1 } else { 0 })[..]),
            Value::Int(v) => self.data.put(&i32::to_be_bytes(*v)[..]),
            Value::BigInt(v) => self.data.put(&i64::to_be_bytes(*v)[..]),
            Value::Varchar(v) => {
                let offset = self.data.len();

                // First two bytes is the offset, which we won't know until build()
                // Second two bytes is the length
                self.data.resize(offset + 4, 0);
                self.data[offset + 2..offset + 4]
                    .copy_from_slice(&u16::to_be_bytes(v.len() as u16));

                self.variable
                    .push(Variable { data: BytesMut::from(&v[..]), offset_offset: offset });
            }
        };

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
            table::tuple::{fit_tuple_with_schema, Builder, Comparand, Data, Value},
        },
        std::cmp::Ordering::{self, *},
    };

    #[test]
    fn test_from() {
        struct Test {
            schema: Schema,
            tuple: Data,
            want: Data,
        }

        let tcs = [
            Test {
                schema: [("col_b", Type::Varchar), ("col_c", Type::Int)].into(),
                tuple: Builder::new()
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::Int(20))
                    .build(),
                want: Builder::new()
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::Int(20))
                    .build(),
            },
            Test {
                schema: [("col_a", Type::Int), ("col_b", Type::Varchar), ("col_c", Type::BigInt)]
                    .into(),
                tuple: Builder::new()
                    .add(&Value::Int(10))
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::BigInt(20))
                    .build(),
                want: Builder::new()
                    .add(&Value::Int(10))
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::BigInt(20))
                    .build(),
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_b".into(), ty: Type::Varchar, offset: 4 },
                    Column { name: "col_c".into(), ty: Type::BigInt, offset: 8 },
                ]),
                tuple: Builder::new()
                    .add(&Value::Int(10))
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::BigInt(20))
                    .build(),
                want: Builder::new()
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::BigInt(20))
                    .build(),
            },
            Test {
                schema: Schema::new(vec![Column {
                    name: "col_b".into(),
                    ty: Type::Varchar,
                    offset: 4,
                }]),
                tuple: Builder::new()
                    .add(&Value::Int(10))
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::BigInt(20))
                    .build(),
                want: Builder::new().add(&Value::Varchar("row_a".into())).build(),
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
                    Column { name: "col_c".into(), ty: Type::BigInt, offset: 8 },
                ]),
                tuple: Builder::new()
                    .add(&Value::Int(10))
                    .add(&Value::Varchar("row_a".into()))
                    .add(&Value::BigInt(20))
                    .build(),
                want: Builder::new().add(&Value::Int(10)).add(&Value::BigInt(20)).build(),
            },
        ];

        for Test { schema, tuple, want } in tcs {
            let have = fit_tuple_with_schema(&tuple.as_bytes(), &schema);
            assert_eq!(want, have);
        }
    }

    #[test]
    fn test_comparator() {
        struct Test {
            schema: Schema,
            lhs: Data,
            rhs: Data,
            want: Ordering,
        }

        let tcs = [
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
                    Column { name: "col_b".into(), ty: Type::Bool, offset: 4 },
                    Column { name: "col_c".into(), ty: Type::BigInt, offset: 5 },
                ]),
                lhs: Builder::new()
                    .add(&Value::Int(4))
                    .add(&Value::Bool(false))
                    .add(&Value::BigInt(100))
                    .build(),
                rhs: Builder::new()
                    .add(&Value::Int(4))
                    .add(&Value::Bool(false))
                    .add(&Value::BigInt(100))
                    .build(),
                want: Equal,
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
                    Column { name: "col_b".into(), ty: Type::Bool, offset: 4 },
                    Column { name: "col_c".into(), ty: Type::BigInt, offset: 5 },
                ]),
                lhs: Builder::new()
                    .add(&Value::Int(4))
                    .add(&Value::Bool(true))
                    .add(&Value::BigInt(100))
                    .build(),
                rhs: Builder::new()
                    .add(&Value::Int(4))
                    .add(&Value::Bool(false))
                    .add(&Value::BigInt(100))
                    .build(),
                want: Greater,
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::Int, offset: 0 },
                    Column { name: "col_b".into(), ty: Type::Bool, offset: 4 },
                    Column { name: "col_c".into(), ty: Type::BigInt, offset: 5 },
                ]),
                lhs: Builder::new()
                    .add(&Value::Int(4))
                    .add(&Value::Bool(false))
                    .add(&Value::BigInt(90))
                    .build(),
                rhs: Builder::new()
                    .add(&Value::Int(4))
                    .add(&Value::Bool(false))
                    .add(&Value::BigInt(100))
                    .build(),
                want: Less,
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::TinyInt, offset: 0 },
                    Column { name: "col_b".into(), ty: Type::Varchar, offset: 1 },
                ]),
                lhs: Builder::new()
                    .add(&Value::TinyInt(1))
                    .add(&Value::Varchar("Column".into()))
                    .build(),
                rhs: Builder::new()
                    .add(&Value::TinyInt(1))
                    .add(&Value::Varchar("Column".into()))
                    .build(),
                want: Equal,
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::Varchar, offset: 0 },
                    Column { name: "col_b".into(), ty: Type::TinyInt, offset: 255 + 2 },
                ]),
                lhs: Builder::new()
                    .add(&Value::Varchar("Column A".into()))
                    .add(&Value::TinyInt(1))
                    .build(),
                rhs: Builder::new()
                    .add(&Value::Varchar("Column B".into()))
                    .add(&Value::TinyInt(1))
                    .build(),
                want: Less,
            },
            Test {
                schema: Schema::new(vec![
                    Column { name: "col_a".into(), ty: Type::Varchar, offset: 0 },
                    Column { name: "col_b".into(), ty: Type::TinyInt, offset: 255 + 2 },
                ]),
                lhs: Builder::new()
                    .add(&Value::Varchar("Column A".into()))
                    .add(&Value::TinyInt(1))
                    .build(),
                rhs: Builder::new()
                    .add(&Value::Varchar("Column".into()))
                    .add(&Value::TinyInt(1))
                    .build(),
                want: Greater,
            },
        ];

        for Test { schema, lhs, rhs, want } in tcs {
            let have = Comparand(&schema, &lhs).cmp(&Comparand(&schema, &rhs));
            assert_eq!(want, have);
        }
    }
}
