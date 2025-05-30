#[derive(PartialEq, Clone, Copy, Debug)]
pub enum Type {
    TinyInt,
    Bool,
    Int,
    BigInt,
    Varchar,
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::TinyInt => write!(f, "TINYINT"),
            Type::Bool => write!(f, "BOOL"),
            Type::Int => write!(f, "INT"),
            Type::BigInt => write!(f, "BIGINT"),
            Type::Varchar => write!(f, "VARCHAR"),
        }
    }
}

impl Type {
    /// Returns the size of any value of the type at tuple level
    /// Since varchar is variable length, we only store the offset and
    /// the size at the tuple level (2 bytes each)
    pub fn size(&self) -> usize {
        match self {
            Type::TinyInt | Type::Bool => 1,
            Type::Int => 4,
            Type::BigInt => 8,
            Type::Varchar => 4,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub offset: usize,
    pub table: Option<String>,
}

#[derive(PartialEq, Clone, Debug, Default)]
pub struct Schema {
    pub columns: Vec<Column>,
    tuple_size: usize,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { tuple_size: columns.iter().fold(0, |acc, column| acc + column.ty.size()), columns }
    }

    /// Returns a new `Schema` which has been filtered to include the specified columns
    pub fn filter(&self, columns: &[&str]) -> Self {
        let mut tuple_size = 0;
        let columns = self
            .iter()
            .filter(|Column { name, .. }| columns.contains(&name.as_str()))
            .map(|col| {
                tuple_size += col.ty.size();
                col.clone()
            })
            .collect();

        Self { columns, tuple_size }
    }

    /// Returns a new `Schema` where the offsets have been adjusted so that all columns are packed
    /// together
    pub fn compact(&self) -> Self {
        let mut schema = self.clone();
        let mut current = 0;

        for Column { ty, offset, .. } in &mut schema.columns {
            *offset = current;
            current += ty.size();
        }

        schema
    }

    /// Returns a new `Schema` where `other` is appended
    pub fn join(&self, other: &Schema) -> Self {
        let mut schema = self.clone();
        schema.columns.extend(other.columns.iter().cloned());
        schema.tuple_size += other.tuple_size;

        schema.compact()
    }

    pub fn qualify(&mut self, table: &str) {
        self.columns.iter_mut().for_each(|column| column.table = Some(table.to_string()));
    }

    pub fn find_column_by_name(&self, column_name: &str) -> Option<&Column> {
        self.columns.iter().find(|Column { name, .. }| name == column_name)
    }

    pub fn find_column_by_name_and_table(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Option<&Column> {
        self.columns.iter().find(|Column { name, table, .. }| {
            table.as_ref().map_or(false, |table| table == table_name) && name == column_name
        })
    }

    pub fn tuple_size(&self) -> usize {
        self.tuple_size
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Column> {
        self.columns.iter()
    }
}

pub struct SchemaBuilder {
    columns: Vec<Column>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn append(&mut self, column: Column) -> &mut Self {
        self.columns.push(column);
        self
    }

    pub fn append_n(&mut self, column: impl IntoIterator<Item = Column>) -> &mut Self {
        self.columns.extend(column.into_iter());
        self
    }

    pub fn build(self) -> Schema {
        Schema::new(self.columns).compact()
    }
}

#[macro_export]
macro_rules! schema {
    () => {
        {
            let columns = Vec::new();
            crate::catalog::schema::Schema::new(columns)
        }
    };
    ( $( $column:expr ),* ) => {
        {
            let mut columns = Vec::new();
            $(
                columns.push($column);
            )*
            crate::catalog::schema::Schema::new(columns).compact()
        }
    };
}

#[macro_export]
macro_rules! column {
    ($name:expr, $ty:tt, $table:expr) => {
        crate::catalog::schema::Column {
            name: $name.into(),
            ty: crate::catalog::schema::Type::$ty,
            offset: 0,
            table: Some($table.into()),
        }
    };
    ($name:expr, $ty:tt) => {
        crate::catalog::schema::Column {
            name: $name.into(),
            ty: crate::catalog::schema::Type::$ty,
            offset: 0,
            table: None,
        }
    };
    ($name:expr => $ty:expr) => {
        crate::catalog::schema::Column { name: $name.into(), ty: $ty, offset: 0, table: None }
    };
}
