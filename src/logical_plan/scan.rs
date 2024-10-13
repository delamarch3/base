use {
    super::{write_list, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct Scan {
    table: String,
    schema: Schema,
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scan: table={}, projection=", self.table,)?;
        write_list(f, &mut self.schema.columns.iter().map(|column| &column.name), ",")?;

        Ok(())
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (None, None)
    }
}

impl Scan {
    pub fn new(table: String, schema: Schema) -> Self {
        Self { table, schema }
    }
}
