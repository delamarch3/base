use super::{expr_type, write_iter, LogicalPlan, LogicalPlanError, LogicalPlanError::*};
use crate::catalog::schema::{Column, Schema, SchemaBuilder, Type};
use crate::column;
use crate::sql::{Expr, Ident, SelectItem};

#[derive(Debug)]
pub struct Projection {
    pub(super) schema: Schema,
    pub(super) input: Box<LogicalPlan>,
    projection: Vec<SelectItem>,
}

/// `schema`, `projection` and `idents` have the same length
/// Each field has a corresponding field at the matching index
/// If the `SelectItem` is an `Ident`, then the corresponding index in `idents` is `Some`
struct ProjectionAttributes {
    schema: Schema,
    projection: Vec<SelectItem>,
    idents: Vec<Option<(usize, Type)>>,
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
    pub fn new(
        projection: Vec<SelectItem>,
        input: impl Into<LogicalPlan>,
    ) -> Result<Self, LogicalPlanError> {
        let input = Box::new(input.into());
        let schema = build_projection_schema(&projection, input.schema())?;

        Ok(Self { schema, input, projection })
    }
}

fn build_projection_schema(
    projection: &Vec<SelectItem>,
    input_schema: &Schema,
) -> Result<Schema, LogicalPlanError> {
    let mut schema = SchemaBuilder::new();
    for item in projection {
        match item {
            SelectItem::Expr(Expr::Ident(ident @ Ident::Single(column))) => schema.append(
                input_schema
                    .find_column_by_name(column)
                    .cloned()
                    .ok_or(UnknownColumn(ident.to_string()))?,
            ),
            SelectItem::Expr(Expr::Ident(ident @ Ident::Compound(idents))) => schema.append(
                input_schema
                    .find_column_by_name_and_table(&idents[0], &idents[1])
                    .cloned()
                    .ok_or(UnknownColumn(ident.to_string()))?,
            ),
            SelectItem::Expr(expr) => {
                schema.append(column!(expr.to_string() => expr_type(expr, input_schema)?))
            }
            SelectItem::AliasedExpr { expr, alias } => {
                schema.append(column!(alias.to_string() => expr_type(expr, input_schema)?))
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

    Ok(schema.build())
}
