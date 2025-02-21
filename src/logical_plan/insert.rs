use std::sync::Arc;

use crate::logical_plan::{LogicalOperator, LogicalOperatorError};

use crate::catalog::schema::Schema;
use crate::catalog::TableInfo;
use crate::{column, schema};

pub struct Insert {
    pub input: Box<LogicalOperator>,
    schema: Schema,
    pub table: Arc<TableInfo>,
}

impl std::fmt::Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Insert table={} oid={}", self.table.name, self.table.oid)?;

        Ok(())
    }
}

impl From<Insert> for LogicalOperator {
    fn from(insert: Insert) -> Self {
        Self::Insert(insert)
    }
}

impl Insert {
    pub fn new(
        table: Arc<TableInfo>,
        input: impl Into<LogicalOperator>,
    ) -> Result<Self, LogicalOperatorError> {
        let input = Box::new(input.into());
        let schema = schema! { column!("ok", Int) };

        let table_schema = &table.schema;
        let insert_schema = input.schema();

        if insert_schema.len() != table_schema.len() {
            Err("insert schema does not match table schema")?
        }

        // TODO: support type coercion
        insert_schema.iter().zip(table_schema.iter()).try_for_each(|(a, b)| {
            (a.ty == b.ty).then_some(()).ok_or_else(|| "insert schema does not match table schema")
        })?;

        Ok(Self { input, schema, table })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}
