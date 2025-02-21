use crate::logical_plan::{write_iter, Expr, LogicalOperator};

pub struct Sort {
    pub exprs: Vec<Expr>,
    pub desc: bool,
    pub input: Box<LogicalOperator>,
}

impl std::fmt::Display for Sort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sort [")?;
        write_iter(f, &mut self.exprs.iter(), ", ")?;
        write!(f, "] {}", if self.desc { "DESC" } else { "ASC" })
    }
}

impl From<Sort> for LogicalOperator {
    fn from(limit: Sort) -> Self {
        Self::Sort(limit)
    }
}

impl Sort {
    pub fn new(exprs: Vec<Expr>, desc: bool, input: impl Into<LogicalOperator>) -> Self {
        Self { exprs, desc, input: Box::new(input.into()) }
    }
}
