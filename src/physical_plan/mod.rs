use crate::catalog::schema::Schema;
use crate::table::tuple::Data as TupleData;

mod create;
mod explain;
mod filter;
mod insert;
mod limit;
mod projection;
mod scan;
mod values;

pub use {
    create::Create, explain::Explain, filter::Filter, insert::Insert, limit::Limit,
    projection::Projection, scan::Scan, values::Values,
};

pub struct ExecutionError(String);
impl std::error::Error for ExecutionError {}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "execution error: {}", self.0)
    }
}

impl std::fmt::Debug for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<String> for ExecutionError {
    fn from(value: String) -> Self {
        ExecutionError(value)
    }
}

pub trait PhysicalOperator {
    fn next(&mut self) -> Result<Option<TupleData>, ExecutionError>;
    fn schema(&self) -> &Schema;
}
