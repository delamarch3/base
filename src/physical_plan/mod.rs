use std::sync::Arc;

use bytes::BytesMut;
use eval::eval;

use crate::catalog::schema::{Column, Schema};
use crate::logical_plan::LogicalPlan;
use crate::logical_plan::ProjectionAttributes;
use crate::sql::{Expr, SelectItem};
use crate::table::list::{Iter as TableIter, ListRef as TableRef};
use crate::table::tuple::Data as TupleData;
use crate::table::tuple::{Builder as TupleBuilder, Value};
use crate::{column, schema};

pub mod eval;

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
        LogicalPlan::Limit(limit) => {
            let input = logical_to_physical(*limit.input)?;
            Box::new(Limit::new(input, limit.limit))
        }
        LogicalPlan::Sort(sort) => todo!(),
        LogicalPlan::Values(values) => Box::new(Values::new(values.values, values.schema)),
        LogicalPlan::Insert(insert) => {
            let input = logical_to_physical(*insert.input)?;
            Box::new(Insert::new(input, Arc::clone(&insert.table.table)))
        }
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

struct Values {
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
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>> {
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

struct Insert {
    table: TableRef,
    schema: Schema,
    input: Box<dyn PhysicalOperator>,
}

impl Insert {
    pub fn new(input: Box<dyn PhysicalOperator>, table: TableRef) -> Self {
        Self { table, input, schema: schema! { column!("ok", Int) } }
    }
}

impl PhysicalOperator for Insert {
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>> {
        while let Some(tuple) = self.input.next()? {
            self.table.insert(&tuple).unwrap();
        }

        Ok(Some(TupleBuilder::new().int(1).build()))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

struct Limit {
    limit: usize,
    pos: usize,
    input: Box<dyn PhysicalOperator>,
}

impl Limit {
    pub fn new(input: Box<dyn PhysicalOperator>, limit: usize) -> Self {
        Self { limit, pos: 0, input }
    }
}

impl PhysicalOperator for Limit {
    fn next(&mut self) -> Result<Option<TupleData>, Box<dyn std::error::Error>> {
        if self.pos == self.limit.saturating_sub(1) {
            return Ok(None);
        }
        self.pos += 1;

        match self.input.next()? {
            Some(tuple) => Ok(Some(tuple)),
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }
}
