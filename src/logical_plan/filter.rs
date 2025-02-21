use crate::logical_plan::{Expr, LogicalOperator};

pub struct Filter {
    pub expr: Expr,
    pub input: Box<LogicalOperator>,
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter [{}]", self.expr)?;

        Ok(())
    }
}

impl From<Filter> for LogicalOperator {
    fn from(filter: Filter) -> Self {
        Self::Filter(filter)
    }
}

impl Filter {
    pub fn new(expr: Expr, input: impl Into<LogicalOperator>) -> Self {
        Self { expr, input: Box::new(input.into()) }
    }
}
