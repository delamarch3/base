use std::sync::Arc;

use crate::{
    logical_plan::LogicalOperator,
    physical_plan::{Filter, Insert, Limit, PhysicalOperator, Projection, Scan, Values},
};

// TODO
/// Apply a set of transformation rules to the `logical_plan`. For example, a transformation rule
/// may push down filters below joins, or it may remove redundant projections.
pub fn transform(logical_plan: LogicalOperator) -> LogicalOperator {
    logical_plan
}

// TODO
/// Apply implementation rules to the `logical_plan`, generating a physical plan that can be
/// executed. For example, a rule could choose a join algorithm, or it could choose  an index
/// scan over a table scan.
pub fn implement(
    logical_plan: LogicalOperator,
) -> Result<Box<dyn PhysicalOperator>, Box<dyn std::error::Error>> {
    let exec: Box<dyn PhysicalOperator> = match logical_plan {
        LogicalOperator::Aggregate(_aggregate) => todo!(),
        LogicalOperator::Filter(filter) => {
            let input = implement(*filter.input)?;
            Box::new(Filter::new(input, filter.expr))
        }
        LogicalOperator::Group(_group) => todo!(),
        LogicalOperator::Join(_join) => todo!(),
        LogicalOperator::Projection(projection) => {
            let input = implement(*projection.input)?;
            Box::new(Projection::new(input, projection.attributes))
        }
        LogicalOperator::Scan(scan) => {
            let iter = scan.table.table.iter().unwrap();
            Box::new(Scan::new(iter, scan.schema))
        }
        LogicalOperator::Limit(limit) => {
            let input = implement(*limit.input)?;
            Box::new(Limit::new(input, limit.limit))
        }
        LogicalOperator::Sort(_sort) => todo!(),
        LogicalOperator::Values(values) => Box::new(Values::new(values.values, values.schema)),
        LogicalOperator::Insert(insert) => {
            let input = implement(*insert.input)?;
            Box::new(Insert::new(input, Arc::clone(&insert.table.table)))
        }
        LogicalOperator::Create(_create) => todo!(),
    };

    Ok(exec)
}
