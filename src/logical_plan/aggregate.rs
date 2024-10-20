use {
    super::{Function, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct Aggregate {
    input: Box<dyn LogicalPlan>,
    function: Function,
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate [{}]", self.function)?;

        Ok(())
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}
