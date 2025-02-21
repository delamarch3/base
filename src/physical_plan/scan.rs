use crate::catalog::schema::Schema;
use crate::physical_plan::{PhysicalOperator, PhysicalOperatorError};
use crate::table::list::Iter as TableIter;
use crate::table::tuple::Data as TupleData;

pub struct Scan {
    iter: TableIter,
    schema: Schema,
}

impl Scan {
    pub fn new(iter: TableIter, schema: Schema) -> Self {
        Self { iter, schema }
    }
}

impl PhysicalOperator for Scan {
    fn next(&mut self) -> Result<Option<TupleData>, PhysicalOperatorError> {
        let next = match self.iter.next() {
            Some(result) => {
                let (_meta, data, _rid) = result.unwrap();
                Some(data)
            }
            None => None,
        };

        Ok(next)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
