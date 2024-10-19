use crate::{
    catalog::TableInfo,
    disk::{Disk, FileSystem},
};

use {
    super::{LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

// TODO: scan doesn't need the actual table pointer, could remove it later to get rid of the generic too
pub struct Scan<D: Disk = FileSystem> {
    table_info: TableInfo<D>,
}

impl<D> std::fmt::Display for Scan<D>
where
    D: Disk,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let TableInfo { name, oid, .. } = &self.table_info;
        write!(f, "Scan: table={} oid={}", name, oid)?;

        Ok(())
    }
}

impl<D> LogicalPlan for Scan<D>
where
    D: Disk,
{
    fn schema(&self) -> &Schema {
        &self.table_info.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (None, None)
    }
}

impl<D> Scan<D>
where
    D: Disk,
{
    pub fn new(table_info: TableInfo<D>) -> Self {
        Self { table_info }
    }
}
