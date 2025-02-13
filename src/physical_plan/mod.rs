use eval::eval;

use crate::catalog::schema::{Column, Schema};
use crate::logical_plan::LogicalPlan;
use crate::logical_plan::ProjectionAttributes;
use crate::sql::{Expr, SelectItem};
use crate::table::list::Iter as TableIter;
use crate::table::tuple::Data as TupleData;
use crate::table::tuple::{Builder as TupleBuilder, Value};

mod eval;

pub fn logical_to_physical(
    logical_plan: LogicalPlan,
) -> Result<Box<dyn PhysicalOperator>, Box<dyn std::error::Error>> {
    let exec: Box<dyn PhysicalOperator> = match logical_plan {
        LogicalPlan::Aggregate(aggregate) => todo!(),
        LogicalPlan::Filter(filter) => {
            let input = logical_to_physical(*filter.input)?;
            Box::new(Filter::new(input, filter.expr))
        }
        LogicalPlan::Group(group) => todo!(),
        LogicalPlan::Join(join) => todo!(),
        LogicalPlan::Projection(projection) => {
            let input = logical_to_physical(*projection.input)?;
            Box::new(Projection::new(input, projection.attributes))
        }
        LogicalPlan::Scan(scan) => {
            let iter = scan.table.table.iter().unwrap();
            Box::new(Scan::new(iter, scan.schema))
        }
        LogicalPlan::Limit(limit) => todo!(),
        LogicalPlan::Sort(sort) => todo!(),
        LogicalPlan::Values(values) => todo!(),
        LogicalPlan::Insert(values) => todo!(),
    };

    Ok(exec)
}

pub trait PhysicalOperator {
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>>;
    fn schema(&self) -> &Schema;
}

pub struct Scan {
    iter: TableIter,
    schema: Schema,
}

impl Scan {
    pub fn new(iter: TableIter, schema: Schema) -> Self {
        Self { iter, schema }
    }
}

impl PhysicalOperator for Scan {
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>> {
        let next = match self.iter.next() {
            Some(result) => {
                let (_meta, data, _rid) = result.unwrap();
                Some(data)
            }
            None => None,
        };

        Ok(next)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

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
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>> {
        loop {
            let Some(input_tuple) = self.input.next()? else { break Ok(None) };
            let value = eval(&self.expr, self.input.schema(), &input_tuple).unwrap();
            match value {
                Value::TinyInt(0) | Value::Bool(false) | Value::Int(0) | Value::BigInt(0) => {
                    continue
                }
                Value::Varchar(s) if s.len() == 0 => continue,
                _ => break Ok(Some(input_tuple)),
            }
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }
}

pub struct Projection {
    attributes: ProjectionAttributes,
    input: Box<dyn PhysicalOperator>,
}

impl Projection {
    pub fn new(input: Box<dyn PhysicalOperator>, attributes: ProjectionAttributes) -> Self {
        Self { attributes, input }
    }
}

impl PhysicalOperator for Projection {
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>> {
        let Some(input_tuple) = self.input.next()? else { return Ok(None) };
        let input_schema = self.input.schema();
        let mut input_idents = self.attributes.input_idents().iter();
        let mut tuple = TupleBuilder::new();
        for select_item in self.attributes.projection() {
            match select_item {
                SelectItem::Expr(Expr::Ident(_)) => {
                    let (ty, offset) = input_idents.next().unwrap();
                    let value = input_tuple.get_value(*offset, *ty);
                    tuple = tuple.add(&value);
                }
                SelectItem::Expr(expr) | SelectItem::AliasedExpr { expr, alias: _ } => {
                    let value = eval(expr, input_schema, &input_tuple).unwrap();
                    tuple = tuple.add(&value);
                }
                SelectItem::Wildcard => {
                    for column in input_schema.iter() {
                        let value = input_tuple.get_value(column.offset, column.ty);
                        tuple = tuple.add(&value);
                    }
                }
                SelectItem::QualifiedWildcard(ident) => {
                    for column in input_schema.columns.iter().filter(|Column { table, .. }| {
                        table.as_ref().map_or(false, |table| table.as_str() == &ident[0])
                    }) {
                        let value = input_tuple.get_value(column.offset, column.ty);
                        tuple = tuple.add(&value);
                    }
                }
            }
        }

        Ok(Some(tuple.build()))
    }

    fn schema(&self) -> &Schema {
        &self.attributes.schema()
    }
}
