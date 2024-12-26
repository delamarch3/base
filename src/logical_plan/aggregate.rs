use super::{write_iter, Expr, Function, LogicalPlan};

#[derive(Debug)]
pub struct Aggregate {
    function: Function,
    keys: Vec<Expr>,
    pub(super) input: Box<LogicalPlan>,
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

impl From<Aggregate> for LogicalPlan {
    fn from(aggregate: Aggregate) -> Self {
        Self::Aggregate(aggregate)
    }
}

impl Aggregate {
    pub fn new(function: Function, keys: Vec<Expr>, input: impl Into<LogicalPlan>) -> Self {
        Self { input: Box::new(input.into()), function, keys }
    }
}
