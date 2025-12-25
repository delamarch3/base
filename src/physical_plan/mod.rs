use crate::catalog::schema::Schema;
use crate::table::tuple::Data as TupleData;

mod create;
mod filter;
mod insert;
mod limit;
mod projection;
mod scan;
mod values;

pub use {
    create::Create, filter::Filter, insert::Insert, limit::Limit, projection::Projection,
    scan::Scan, values::Values,
};

pub struct PhysicalOperatorError(String);
impl std::error::Error for PhysicalOperatorError {}

impl std::fmt::Display for PhysicalOperatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "physical operator error: {}", self.0)
    }
}

impl std::fmt::Debug for PhysicalOperatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<String> for PhysicalOperatorError {
    fn from(value: String) -> Self {
        PhysicalOperatorError(value)
    }
}

pub trait PhysicalOperator {
    fn next(&mut self) -> Result<Option<TupleData>, PhysicalOperatorError>;
    fn schema(&self) -> &Schema;
}
