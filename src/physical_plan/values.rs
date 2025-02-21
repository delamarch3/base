use crate::catalog::schema::Schema;
use crate::execution::eval;
use crate::physical_plan::{PhysicalOperator, PhysicalOperatorError};
use crate::schema;
use crate::sql::Expr;
use crate::table::tuple::{Builder as TupleBuilder, Data as TupleData};

pub struct Values {
    values: Vec<Vec<Expr>>,
    schema: Schema,
    pos: usize,
}

impl Values {
    pub fn new(values: Vec<Vec<Expr>>, schema: Schema) -> Self {
        Self { values, schema, pos: 0 }
    }
}

impl PhysicalOperator for Values {
    fn next(&mut self) -> Result<Option<TupleData>, PhysicalOperatorError> {
        self.pos += 1;
        let Some(values) = self.values.get(self.pos - 1) else { return Ok(None) };

        let mut tuple = TupleBuilder::new();
        for (i, _column) in self.schema.iter().enumerate() {
            let value = eval(&values[i], &schema! {}, &TupleData::empty()).unwrap();
            tuple = tuple.add(&value);
        }

        Ok(Some(tuple.build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
