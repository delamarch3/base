use {
    super::{Expr, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct Filter {
    expr: Expr,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: expr={}", self.expr)?;

        Ok(())
    }
}

impl LogicalPlan for Filter {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Filter {
    pub fn new(expr: Expr, input: Box<dyn LogicalPlan>) -> Self {
        Self { expr, input }
    }
}
