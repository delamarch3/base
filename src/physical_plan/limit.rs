use crate::catalog::schema::Schema;
use crate::physical_plan::{ExecutionError, PhysicalOperator};
use crate::table::tuple::Data as TupleData;

pub struct Limit {
    limit: usize,
    pos: usize,
    input: Box<dyn PhysicalOperator>,
}

impl Limit {
    pub fn new(input: Box<dyn PhysicalOperator>, limit: usize) -> Self {
        Self { limit, pos: 0, input }
    }
}

impl PhysicalOperator for Limit {
    fn next(&mut self) -> Result<Option<TupleData>, ExecutionError> {
        if self.pos == self.limit.saturating_sub(1) {
            return Ok(None);
        }
        self.pos += 1;

        match self.input.next()? {
            Some(tuple) => Ok(Some(tuple)),
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }
}
