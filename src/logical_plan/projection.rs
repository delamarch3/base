use super::{write_iter, LogicalPlan};
use crate::catalog::schema::{Schema, SchemaBuilder, Type};
use crate::sql::SelectItem;

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
            SelectItem::Expr(expr) => {
                // expr.type()
                schema.append((expr.to_string(), Type::Bool))
            }
            SelectItem::AliasedExpr { expr, alias } => {
                // expr.type();
                schema.append((alias.to_string(), Type::Bool))
            }
            SelectItem::Wildcard => schema.append_schema(input_schema),
            SelectItem::QualifiedWildcard(_) => todo!(),
        };
    }

    schema.build()
}
