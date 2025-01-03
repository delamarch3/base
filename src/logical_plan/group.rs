use super::{write_iter, Expr, LogicalPlan};

#[derive(Debug)]
pub struct Group {
    keys: Vec<Expr>,
    pub(super) input: Box<LogicalPlan>,
}

impl std::fmt::Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Group keys:[")?;
        write_iter(f, &mut self.keys.iter(), ",")?;
        write!(f, "]")
    }
}

impl From<Group> for LogicalPlan {
    fn from(group: Group) -> Self {
        Self::Group(group)
    }
}

impl Group {
    pub fn new(keys: Vec<Expr>, input: impl Into<LogicalPlan>) -> Self {
        Self { keys, input: Box::new(input.into()) }
    }
}
