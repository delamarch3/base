use std::sync::Arc;

use crate::logical_plan::{LogicalPlan, LogicalPlanError, LogicalPlanError::*};

use crate::catalog::schema::Schema;
use crate::catalog::TableInfo;
use crate::{column, schema};

pub struct Insert {
    pub input: Box<LogicalPlan>,
    schema: Schema,
    pub table: Arc<TableInfo>,
}

impl std::fmt::Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Insert table={} oid={}", self.table.name, self.table.oid)?;

        Ok(())
    }
}

impl From<Insert> for LogicalPlan {
    fn from(insert: Insert) -> Self {
        Self::Insert(insert)
    }
}

impl Insert {
    pub fn new(
        table: Arc<TableInfo>,
        input: impl Into<LogicalPlan>,
    ) -> Result<Self, LogicalPlanError> {
        let input = Box::new(input.into());
        let schema = schema! { column!("ok", Int) };

        let table_schema = &table.schema;
        let input_schema = input.schema();

        if input_schema.len() != table_schema.len() {
            Err(SchemaMismatch)?
        }

        // TODO: support type coercion
        input_schema
            .iter()
            .zip(table_schema.iter())
            .try_for_each(|(a, b)| (a.ty == b.ty).then_some(()).ok_or_else(|| SchemaMismatch))?;

        Ok(Self { input, schema, table })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}
