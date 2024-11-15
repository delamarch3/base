use {
    super::{write_iter, LogicalPlan},
    crate::catalog::Schema,
};

// TODO: keeping this simple for now but should be able to support expressions too
pub struct Projection {
    pub(super) schema: Schema,
    pub(super) input: Box<LogicalPlan>,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Projection [")?;
        write_iter(f, &mut self.schema.columns.iter().map(|column| &column.name), ",")?;
        write!(f, "]")
    }
}

impl From<Projection> for LogicalPlan {
    fn from(projection: Projection) -> Self {
        Self::Projection(projection)
    }
}

impl Projection {
    pub fn new(exprs: &[&str], input: impl Into<LogicalPlan>) -> Self {
        let input = Box::new(input.into());
        let schema = input.schema().filter(exprs);
        Self { schema, input }
    }
}
