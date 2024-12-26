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
        // TODO: allow qualifiying with an alias
        let TableInfo { name, schema, oid, .. } = table_info;
        let mut schema = schema.clone();
        schema.qualify(&name);
        Self { name: name.clone(), oid: *oid, schema }
    }
}
