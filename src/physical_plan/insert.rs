use crate::catalog::schema::Schema;
use crate::physical_plan::{ExecutionError, PhysicalOperator};
use crate::table::list::ListRef as TableRef;
use crate::table::tuple::{Builder as TupleBuilder, Data as TupleData};
use crate::{column, schema};

pub struct Insert {
    table: TableRef,
    schema: Schema,
    input: Box<dyn PhysicalOperator>,
    invoked: bool,
}

impl Insert {
    pub fn new(input: Box<dyn PhysicalOperator>, table: TableRef) -> Self {
        Self { table, input, schema: schema! { column!("ok", Int) }, invoked: false }
    }
}

impl PhysicalOperator for Insert {
    fn next(&mut self) -> Result<Option<TupleData>, ExecutionError> {
        if self.invoked {
            return Ok(None);
        }

        while let Some(tuple) = self.input.next()? {
            self.table.insert(&tuple).unwrap();
        }

        self.invoked = true;

        Ok(Some(TupleBuilder::new().int(1).build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
