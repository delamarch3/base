use crate::{
    catalog::{schema::Schema, SharedCatalog},
    physical_plan::{ExecutionError, PhysicalOperator},
    schema,
    table::tuple::{Builder as TupleBuilder, Data as TupleData},
};

pub struct Create {
    catalog: SharedCatalog,
    name: String,
    schema: Schema,
    table_schema: Schema,
    invoked: bool,
}

impl Create {
    pub fn new(catalog: SharedCatalog, name: String, table_schema: Schema) -> Self {
        Self { catalog, name, table_schema, schema: schema! { ok Int }, invoked: false }
    }
}

impl PhysicalOperator for Create {
    fn next(&mut self) -> Result<Option<TupleData>, ExecutionError> {
        if self.invoked {
            return Ok(None);
        }

        let mut catalog = self.catalog.lock().unwrap();

        let Create { name, table_schema, .. } = &self;

        if catalog
            .create_table(name, table_schema.clone())
            .map_err(|e| ExecutionError(e.to_string()))?
            .is_none()
        {
            Err(format!("{name} already exists"))?
        };

        self.invoked = true;

        Ok(Some(TupleBuilder::new().int(1).build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
