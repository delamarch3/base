use crate::execution::eval;
use crate::logical_plan::{Expr, LogicalOperator, LogicalOperatorError};
use crate::schema;
use crate::table::tuple::Data as TupleData;
use crate::table::tuple::Value;

pub struct Limit {
    pub limit: usize,
    pub input: Box<LogicalOperator>,
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Limit {}", self.limit)
    }
}

impl From<Limit> for LogicalOperator {
    fn from(limit: Limit) -> Self {
        Self::Limit(limit)
    }
}

impl Limit {
    pub fn new(
        expr: Expr,
        input: impl Into<LogicalOperator>,
    ) -> Result<Self, LogicalOperatorError> {
        let mut limit = eval(&expr, &schema! {}, &TupleData::empty())
            .map(|value| match value {
                Value::TinyInt(limit) => limit as isize,
                Value::Bool(bool) if bool => 1,
                Value::Bool(_) => 0,
                Value::Int(limit) => limit as isize,
                Value::BigInt(limit) => limit as isize,
                Value::Varchar(_) => todo!(),
            })
            .map_err(|_| "limit expr must be static")?;
        if limit.is_negative() {
            limit = 0;
        }

        Ok(Self { limit: limit as usize, input: Box::new(input.into()) })
    }
}
