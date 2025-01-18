use std::sync::Arc;

use crate::catalog::schema::Schema;
use crate::catalog::TableInfo;

use super::LogicalPlan;

pub struct Scan {
    pub table: Arc<TableInfo>,
    pub schema: Schema,
    pub alias: Option<String>,
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Scan table={} alias={} oid={}",
            self.table.name,
            match self.alias.as_ref() {
                Some(alias) => alias,
                None => "",
            },
            self.table.oid
        )?;

        Ok(())
    }
}

impl From<Scan> for LogicalPlan {
    fn from(scan: Scan) -> Self {
        Self::Scan(scan)
    }
}

impl Scan {
    pub fn new(table: Arc<TableInfo>) -> Self {
        let mut schema = table.schema.clone();
        schema.qualify(&table.name);
        Self { table, schema, alias: None }
    }

    pub fn new_with_alias(table: Arc<TableInfo>, alias: String) -> Self {
        let mut schema = table.schema.clone();
        schema.qualify(&alias);
        Self { table, schema, alias: Some(alias) }
    }
}
