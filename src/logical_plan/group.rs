use {
    super::{write_iter, Expr, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct Group {
    keys: Vec<Expr>,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Group keys:[")?;
        write_iter(f, &mut self.keys.iter(), ",")?;
        write!(f, "]")
    }
}

impl LogicalPlan for Group {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Group {
    pub fn new(keys: Vec<Expr>, input: Box<dyn LogicalPlan>) -> Self {
        Self { keys, input }
    }
}
