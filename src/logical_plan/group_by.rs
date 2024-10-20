use super::write_iter;

use {
    super::{Expr, Function, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct GroupBy {
    input: Box<dyn LogicalPlan>,
    function: Function,
    group: Vec<Expr>,
}

impl std::fmt::Display for GroupBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate [{}] groups:[", self.function)?;
        write_iter(f, &mut self.group.iter(), ",")?;
        write!(f, "]")
    }
}

impl LogicalPlan for GroupBy {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}
