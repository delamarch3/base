use crate::logical_plan::{write_iter, Expr, LogicalPlan};

pub struct Sort {
    pub exprs: Vec<Expr>,
    pub desc: bool,
    pub input: Box<LogicalPlan>,
}

impl std::fmt::Display for Sort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sort [")?;
        write_iter(f, &mut self.exprs.iter(), ", ")?;
        write!(f, "] {}", if self.desc { "DESC" } else { "ASC" })
    }
}

impl From<Sort> for LogicalPlan {
    fn from(limit: Sort) -> Self {
        Self::Sort(limit)
    }
}

impl Sort {
    pub fn new(exprs: Vec<Expr>, desc: bool, input: impl Into<LogicalPlan>) -> Self {
        Self { exprs, desc, input: Box::new(input.into()) }
    }
}
