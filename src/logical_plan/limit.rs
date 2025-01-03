use super::{Expr, LogicalPlan};

#[derive(Debug)]
pub struct Limit {
    expr: Expr,
    pub(super) input: Box<LogicalPlan>,
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit {}", self.expr)
    }
}

impl From<Limit> for LogicalPlan {
    fn from(limit: Limit) -> Self {
        Self::Limit(limit)
    }
}

impl Limit {
    pub fn new(expr: Expr, input: impl Into<LogicalPlan>) -> Self {
        Self { expr, input: Box::new(input.into()) }
    }
}
