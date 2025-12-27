use crate::{
    catalog::schema::Schema,
    logical_plan::LogicalOperator,
    physical_plan::{ExecutionError, PhysicalOperator},
    table::tuple::{Builder as TupleBuilder, Data as TupleData},
};

pub struct Explain {
    schema: Schema,
    input: LogicalOperator,
    invoked: bool,
}

impl Explain {
    pub fn new(input: LogicalOperator, schema: Schema) -> Self {
        Self { input, schema, invoked: false }
    }
}

impl PhysicalOperator for Explain {
    fn next(&mut self) -> Result<Option<TupleData>, ExecutionError> {
        if self.invoked {
            return Ok(None);
        }

        let plan = self.input.to_string();
        self.invoked = true;

        Ok(Some(TupleBuilder::new().varchar(&plan).build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
