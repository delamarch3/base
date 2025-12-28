use crate::{catalog::schema::Schema, logical_plan::LogicalOperator, schema};

pub struct Explain {
    pub input: Box<LogicalOperator>,
    pub schema: Schema,
}

impl std::fmt::Display for Explain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Explain")
    }
}

impl From<Explain> for LogicalOperator {
    fn from(create: Explain) -> Self {
        Self::Explain(create)
    }
}

impl Explain {
    pub fn new(input: impl Into<LogicalOperator>) -> Self {
        let input = Box::new(input.into());
        Self { input, schema: schema! { plan Varchar } }
    }
}
