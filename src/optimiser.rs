use std::sync::Arc;

use crate::{
    catalog::SharedCatalog,
    logical_plan::LogicalOperator,
    physical_plan::{
        Create, Explain, Filter, Insert, Limit, PhysicalOperator, Projection, Scan, Values,
    },
};

pub struct Optimiser {
    catalog: SharedCatalog,
}

impl Optimiser {
    pub fn new(catalog: SharedCatalog) -> Self {
        Self { catalog }
    }

    // TODO
    /// Apply a set of transformation rules to the `logical_plan`. For example, a transformation rule
    /// may push down filters below joins, or it may remove redundant projections.
    pub fn transform(&self, logical_plan: LogicalOperator) -> LogicalOperator {
        logical_plan
    }

    // TODO
    /// Apply implementation rules to the `logical_plan`, generating a physical plan that can be
    /// executed. For example, a rule could choose a join algorithm, or it could choose  an index
    /// scan over a table scan.
    pub fn implement(&self, logical_plan: LogicalOperator) -> Box<dyn PhysicalOperator> {
        let exec: Box<dyn PhysicalOperator> = match logical_plan {
            LogicalOperator::Aggregate(_aggregate) => todo!(),
            LogicalOperator::Filter(filter) => {
                let input = self.implement(*filter.input);
                Box::new(Filter::new(input, filter.expr))
            }
            LogicalOperator::Group(_group) => todo!(),
            LogicalOperator::Join(_join) => todo!(),
            LogicalOperator::Projection(projection) => {
                let input = self.implement(*projection.input);
                Box::new(Projection::new(input, projection.attributes))
            }
            LogicalOperator::Scan(scan) => {
                let iter = scan.table.table.iter().unwrap();
                Box::new(Scan::new(iter, scan.schema))
            }
            LogicalOperator::Limit(limit) => {
                let input = self.implement(*limit.input);
                Box::new(Limit::new(input, limit.limit))
            }
            LogicalOperator::Sort(_sort) => todo!(),
            LogicalOperator::Values(values) => Box::new(Values::new(values.values, values.schema)),
            LogicalOperator::Insert(insert) => {
                let input = self.implement(*insert.input);
                Box::new(Insert::new(input, Arc::clone(&insert.table.table)))
            }
            LogicalOperator::Create(create) => {
                Box::new(Create::new(Arc::clone(&self.catalog), create.name, create.schema))
            }
            LogicalOperator::Explain(explain) => {
                Box::new(Explain::new(*explain.input, explain.schema))
            }
        };

        exec
    }
}
