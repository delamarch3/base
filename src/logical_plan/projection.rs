use super::{write_iter, LogicalPlan, LogicalPlanError, LogicalPlanError::*};
use crate::catalog::{Column, Schema, SchemaBuilder, Type};
use crate::sql::{Expr, FunctionName, SelectItem};

pub struct Projection {
    pub(super) schema: Schema,
    pub(super) input: Box<LogicalPlan>,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Projection [")?;
        write_iter(f, &mut self.schema.columns.iter().map(|column| &column.name), ", ")?;
        write!(f, "]")
    }
}

impl From<Projection> for LogicalPlan {
    fn from(projection: Projection) -> Self {
        Self::Projection(projection)
    }
}

impl Projection {
    pub fn new(projection: &[SelectItem], input: impl Into<LogicalPlan>) -> Self {
        let input = Box::new(input.into());

        let mut schema = SchemaBuilder::new();
        for item in projection {
            match item {
                SelectItem::Expr(expr) => {
                    // expr.type()
                    schema.append((expr.to_string(), Type::Bool))
                }
                SelectItem::AliasedExpr { expr, alias } => {
                    // expr.type();
                    schema.append((alias.to_string(), Type::Bool))
                }
                SelectItem::Wildcard => schema.append_schema(input.schema()),
                SelectItem::QualifiedWildcard(_) => todo!(),
            };
        }

        Self { schema: schema.build(), input }
    }
}
