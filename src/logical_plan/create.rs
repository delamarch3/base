use crate::catalog::schema::Schema;
use crate::logical_plan::LogicalOperator;

pub struct Create {
    name: String,
    schema: Schema,
}

impl std::fmt::Display for Create {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Create table={} schema=[", self.name)?;

        let mut sep = "";
        for column in &self.schema.columns {
            write!(f, "{sep}{column}")?;
            sep = ", ";
        }

        write!(f, "]")
    }
}

impl From<Create> for LogicalOperator {
    fn from(create: Create) -> Self {
        Self::Create(create)
    }
}

impl Create {
    pub fn new(name: String, schema: Schema) -> Self {
        Self { name, schema }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}
