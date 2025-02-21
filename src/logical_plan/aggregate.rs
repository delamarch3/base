use crate::logical_plan::{write_iter, Expr, Function, LogicalOperator};

pub struct Aggregate {
    pub function: Function,
    pub keys: Vec<Expr>,
    pub input: Box<LogicalOperator>,
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate [{}]", self.function)?;
        if !self.keys.is_empty() {
            write!(f, " keys:[")?;
            write_iter(f, &mut self.keys.iter(), ",")?;
        }

        write!(f, "]")
    }
}

impl From<Aggregate> for LogicalOperator {
    fn from(aggregate: Aggregate) -> Self {
        Self::Aggregate(aggregate)
    }
}

impl Aggregate {
    pub fn new(function: Function, keys: Vec<Expr>, input: impl Into<LogicalOperator>) -> Self {
        Self { input: Box::new(input.into()), function, keys }
    }
}
