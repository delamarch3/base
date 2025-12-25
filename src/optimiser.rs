use std::sync::Arc;

use crate::logical_plan::LogicalOperator;
use crate::physical_plan::{Filter, Insert, Limit, PhysicalOperator, Projection, Scan, Values};

// TODO: implement tranformation rules (on the logical plan)
// and implementation rules (logical -> physical)
pub fn logical_to_physical(
    logical_plan: LogicalOperator,
) -> Result<Box<dyn PhysicalOperator>, Box<dyn std::error::Error>> {
    let exec: Box<dyn PhysicalOperator> = match logical_plan {
        LogicalOperator::Aggregate(_aggregate) => todo!(),
        LogicalOperator::Filter(filter) => {
            let input = logical_to_physical(*filter.input)?;
            Box::new(Filter::new(input, filter.expr))
        }
        LogicalOperator::Group(_group) => todo!(),
        LogicalOperator::Join(_join) => todo!(),
        LogicalOperator::Projection(projection) => {
            let input = logical_to_physical(*projection.input)?;
            Box::new(Projection::new(input, projection.attributes))
        }
        LogicalOperator::Scan(scan) => {
            let iter = scan.table.table.iter().unwrap();
            Box::new(Scan::new(iter, scan.schema))
        }
        LogicalOperator::Limit(limit) => {
            let input = logical_to_physical(*limit.input)?;
            Box::new(Limit::new(input, limit.limit))
        }
        LogicalOperator::Sort(_sort) => todo!(),
        LogicalOperator::Values(values) => Box::new(Values::new(values.values, values.schema)),
        LogicalOperator::Insert(insert) => {
            let input = logical_to_physical(*insert.input)?;
            Box::new(Insert::new(input, Arc::clone(&insert.table.table)))
        }
        LogicalOperator::Create(_create) => todo!(),
    };

    Ok(exec)
}
