use crate::{
    catalog::schema::{Column, Schema},
    evaluation::eval,
    logical_plan::ProjectionAttributes,
    physical_plan::{ExecutionError, PhysicalOperator},
    sql::{Expr, SelectItem},
    table::tuple::{Builder as TupleBuilder, Data as TupleData},
};

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
    fn next(&mut self) -> Result<Option<TupleData>, ExecutionError> {
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
                        table.as_ref().is_some_and(|table| table.as_str() == &ident[0])
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
        self.attributes.schema()
    }
}
