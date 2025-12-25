use crate::{
    catalog::{schema::Schema, Catalog},
    column,
    physical_plan::{PhysicalOperator, PhysicalOperatorError},
    schema,
    table::tuple::{Builder as TupleBuilder, Data as TupleData},
};

pub struct Create {
    _name: String,
    schema: Schema,
    _table_schema: Schema,
}

impl Create {
    pub fn new(name: String, table_schema: Schema) -> Self {
        Self { _name: name, _table_schema: table_schema, schema: schema! { column!("ok", Int) } }
    }
}

impl PhysicalOperator for Create {
    fn next(&mut self) -> Result<Option<TupleData>, PhysicalOperatorError> {
        // TODO: planner.rs creates the table - need to set up an Arc<Catalog> to use it inside
        // operators
        Ok(Some(TupleBuilder::new().int(1).build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
