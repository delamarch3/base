#[derive(PartialEq, Clone, Copy, Debug)]
pub enum Type {
    TinyInt,
    Bool,
    Int,
    BigInt,
    Varchar,
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
}

impl Column {
    pub fn value_size(&self) -> usize {
        self.ty.size()
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
pub struct Schema {
    pub columns: Vec<Column>,
    tuple_size: usize,
}

impl<const N: usize> From<[(&str, Type); N]> for Schema {
    fn from(fields: [(&str, Type); N]) -> Self {
        let mut columns = Vec::new();

        let mut offset = 0;
        for (name, ty) in fields {
            columns.push(Column { name: name.to_string(), ty, offset });
            offset += ty.size();
        }

        Self { tuple_size: offset, columns }
    }
}

impl From<Vec<(String, Type)>> for Schema {
    fn from(fields: Vec<(String, Type)>) -> Self {
        let mut columns = Vec::new();

        let mut offset = 0;
        for (name, ty) in fields {
            columns.push(Column { name, ty, offset });
            offset += ty.size();
        }

        Self { tuple_size: offset, columns }
    }
}

impl Schema {
    // TODO: support nullable columns
    pub fn new(columns: Vec<Column>) -> Self {
        Self { tuple_size: columns.iter().fold(0, |acc, c| acc + c.value_size()), columns }
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

    /// Returns a new `Schema` where another `Schema` is appended
    pub fn join(&self, other: &Schema) -> Self {
        let mut schema = self.clone();
        schema.columns.extend(other.columns.iter().cloned());
        schema.tuple_size += other.tuple_size;

        schema
    }

    pub fn find(&self, column_name: &str) -> Option<&Column> {
        self.columns.iter().find(|Column { name, .. }| name == column_name)
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
    columns: Vec<(String, Type)>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn append(&mut self, column: (String, Type)) -> &mut Self {
        self.columns.push(column);
        self
    }

    pub fn append_n(&mut self, column: &[(String, Type)]) -> &mut Self {
        self.columns.extend(column.iter().cloned());
        self
    }

    pub fn append_schema(&mut self, schema: &Schema) -> &mut Self {
        self.columns
            .extend(schema.columns.iter().cloned().map(|Column { name, ty, .. }| (name, ty)));
        self
    }

    pub fn build(self) -> Schema {
        self.columns.into()
    }
}
