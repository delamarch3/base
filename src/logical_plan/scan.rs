use crate::{
    catalog::{TableInfo, OID},
    disk::{Disk, FileSystem},
};

use {super::LogicalPlan, crate::catalog::Schema};

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
        Self { name: name.clone(), oid: *oid, schema: schema.clone() }
    }
}
