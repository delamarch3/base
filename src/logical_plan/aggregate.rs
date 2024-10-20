use {
    super::{write_iter, Expr, Function, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct Aggregate {
    function: Function,
    keys: Vec<Expr>,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Aggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate [{}]", self.function)?;
        if self.keys.len() > 0 {
            write!(f, " keys:[")?;
            write_iter(f, &mut self.keys.iter(), ",")?;
        }

        write!(f, "]")
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Aggregate {
    pub fn new(function: Function, keys: Vec<Expr>, input: Box<dyn LogicalPlan>) -> Self {
        Self { input, function, keys }
    }
}
