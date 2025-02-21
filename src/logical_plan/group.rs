use crate::logical_plan::{write_iter, Expr, LogicalOperator};

pub struct Group {
    pub keys: Vec<Expr>,
    pub input: Box<LogicalOperator>,
}

impl std::fmt::Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Group keys:[")?;
        write_iter(f, &mut self.keys.iter(), ",")?;
        write!(f, "]")
    }
}

impl From<Group> for LogicalOperator {
    fn from(group: Group) -> Self {
        Self::Group(group)
    }
}

impl Group {
    pub fn new(keys: Vec<Expr>, input: impl Into<LogicalOperator>) -> Self {
        Self { keys, input: Box::new(input.into()) }
    }
}
