use crate::{
    catalog::schema::Schema,
    evaluation::eval,
    physical_plan::{PhysicalOperator, PhysicalOperatorError},
    sql::Expr,
    table::tuple::{Data as TupleData, Value},
};

pub struct Filter {
    expr: Expr,
    input: Box<dyn PhysicalOperator>,
}

impl Filter {
    pub fn new(input: Box<dyn PhysicalOperator>, expr: Expr) -> Self {
        Self { input, expr }
    }
}

impl PhysicalOperator for Filter {
    fn next(&mut self) -> Result<Option<TupleData>, PhysicalOperatorError> {
        loop {
            let Some(input_tuple) = self.input.next()? else { break Ok(None) };
            let value = eval(&self.expr, self.input.schema(), &input_tuple).unwrap();
            match value {
                Value::TinyInt(0) | Value::Bool(false) | Value::Int(0) | Value::BigInt(0) => {
                    continue
                }
                Value::Varchar(s) if s.is_empty() => continue,
                _ => break Ok(Some(input_tuple)),
            }
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }
}
