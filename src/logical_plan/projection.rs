use super::{write_iter, LogicalPlan};
use crate::catalog::schema::{Column, Schema, SchemaBuilder};
use crate::column;
use crate::sql::{Expr, SelectItem};

#[derive(Debug)]
pub struct Projection {
    pub(super) schema: Schema,
    pub(super) input: Box<LogicalPlan>,
    projection: Vec<SelectItem>,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Projection [")?;
        write_iter(f, &mut self.projection.iter(), ", ")?;
        write!(f, "]")
    }
}

impl From<Projection> for LogicalPlan {
    fn from(projection: Projection) -> Self {
        Self::Projection(projection)
    }
}

impl Projection {
    pub fn new(projection: Vec<SelectItem>, input: impl Into<LogicalPlan>) -> Self {
        let input = Box::new(input.into());
        let schema = build_projection_schema(&projection, input.schema());

        Self { schema, input, projection }
    }
}

pub fn build_projection_schema(projection: &Vec<SelectItem>, input_schema: &Schema) -> Schema {
    let mut schema = SchemaBuilder::new();
    for item in projection {
        match item {
            // SelectItem::Expr(Expr::Ident(ident)) => {
            // pick from schema and keep table ref

            // expr.type()
            // schema.append((expr.to_string(), Type::Bool))
            // todo!()
            // }
            SelectItem::Expr(expr) => {
                // expr.type()
                schema.append(column!(expr.to_string(), Bool))
            }
            SelectItem::AliasedExpr { expr, alias } => {
                // expr.type();
                schema.append(column!(alias.to_string(), Bool))
            }
            SelectItem::Wildcard => schema.append_n(input_schema.columns.clone()),
            SelectItem::QualifiedWildcard(ident) => schema.append_n(
                input_schema
                    .columns
                    .iter()
                    .filter(|Column { table, .. }| {
                        table.as_ref().map_or(false, |table| table.as_str() == &ident[0])
                    })
                    .cloned(),
            ),
        };
    }

    schema.build()
}
