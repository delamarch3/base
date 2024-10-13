use {
    super::{write_list, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

// TODO: keeping this simple for now but should be able to support expressions too
pub struct Projection {
    schema: Schema,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Projection: ")?;
        write_list(f, &mut self.schema.columns.iter().map(|column| &column.name), ",")?;

        Ok(())
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Projection {
    pub fn new(exprs: &[&str], input: Box<dyn LogicalPlan>) -> Self {
        let schema = input.schema().filter(exprs);
        Self { schema, input }
    }
}
