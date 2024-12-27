use crate::catalog::schema::Schema;
use crate::catalog::{TableInfo, OID};
use crate::disk::Disk;

use super::LogicalPlan;

#[derive(Debug)]
pub struct Scan {
    name: String,
    oid: OID,
    pub(super) schema: Schema,
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scan {} {}", self.name, self.oid)?;

        Ok(())
    }
}

impl From<Scan> for LogicalPlan {
    fn from(scan: Scan) -> Self {
        Self::Scan(scan)
    }
}

impl Scan {
    pub fn new<D: Disk>(table_info: &TableInfo<D>) -> Self {
        let TableInfo { name, schema, oid, .. } = table_info;
        let mut schema = schema.clone();
        schema.qualify(&name);
        Self { name: name.clone(), oid: *oid, schema }
    }

    pub fn new_with_alias<D: Disk>(table_info: &TableInfo<D>, alias: String) -> Self {
        let TableInfo { schema, oid, .. } = table_info;
        let mut schema = schema.clone();
        schema.qualify(&alias);
        Self { name: alias.clone(), oid: *oid, schema }
    }
}
