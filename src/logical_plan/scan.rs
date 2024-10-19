use {
    super::{LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct Scan {
    table: String,
    schema: Schema,
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scan: table={}", self.table)?;

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
