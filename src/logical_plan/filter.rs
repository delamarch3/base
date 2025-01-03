use super::{Expr, LogicalPlan};

#[derive(Debug)]
pub struct Filter {
    expr: Expr,
    pub(super) input: Box<LogicalPlan>,
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter [{}]", self.expr)?;

        Ok(())
    }
}

impl From<Filter> for LogicalPlan {
    fn from(filter: Filter) -> Self {
        Self::Filter(filter)
    }
}

impl Filter {
    pub fn new(expr: Expr, input: impl Into<LogicalPlan>) -> Self {
        Self { expr, input: Box::new(input.into()) }
    }
}
