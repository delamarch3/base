use crate::catalog::schema::Schema;
use crate::physical_plan::{PhysicalOperator, PhysicalOperatorError};
use crate::table::list::ListRef as TableRef;
use crate::table::tuple::{Builder as TupleBuilder, Data as TupleData};
use crate::{column, schema};

pub struct Insert {
    table: TableRef,
    schema: Schema,
    input: Box<dyn PhysicalOperator>,
}

impl Insert {
    pub fn new(input: Box<dyn PhysicalOperator>, table: TableRef) -> Self {
        Self { table, input, schema: schema! { column!("ok", Int) } }
    }
}

impl PhysicalOperator for Insert {
    fn next(&mut self) -> Result<Option<TupleData>, PhysicalOperatorError> {
        while let Some(tuple) = self.input.next()? {
            self.table.insert(&tuple).unwrap();
        }

        Ok(Some(TupleBuilder::new().int(1).build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
