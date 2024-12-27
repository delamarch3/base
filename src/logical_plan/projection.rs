use super::{write_iter, LogicalPlan};
use crate::catalog::schema::{Column, Schema, SchemaBuilder, Type};
use crate::column;
use crate::sql::{Expr, FunctionName, Ident, Literal, Op, SelectItem};

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

fn build_projection_schema(projection: &Vec<SelectItem>, input_schema: &Schema) -> Schema {
    let mut schema = SchemaBuilder::new();
    for item in projection {
        match item {
            SelectItem::Expr(Expr::Ident(Ident::Single(column))) => {
                schema.append(input_schema.find_column_by_name(column).cloned().expect("todo"))
            }
            SelectItem::Expr(Expr::Ident(Ident::Compound(idents))) => schema.append(
                input_schema
                    .find_column_by_name_and_table(&idents[0], &idents[1])
                    .cloned()
                    .expect("todo"),
            ),
            SelectItem::Expr(expr) => {
                schema.append(column!(expr.to_string() => expr_type(expr, input_schema)))
            }
            SelectItem::AliasedExpr { expr, alias } => {
                schema.append(column!(alias.to_string() => expr_type(expr, input_schema)))
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

fn expr_type(expr: &Expr, schema: &Schema) -> Type {
    match expr {
        Expr::Ident(Ident::Single(column)) => schema.find_column_by_name(column).expect("todo").ty,
        Expr::Ident(Ident::Compound(idents)) => {
            schema.find_column_by_name_and_table(&idents[0], &idents[1]).expect("todo").ty
        }
        Expr::Literal(literal) => match literal {
            Literal::Number(_) => Type::Int,
            Literal::Decimal(_) => todo!(),
            Literal::String(_) => Type::Varchar,
            Literal::Bool(_) => Type::Bool,
            Literal::Null => todo!(),
        },
        Expr::IsNull { .. } | Expr::InList { .. } | Expr::Between { .. } => Type::Bool,
        Expr::BinaryOp { left: _, op, right: _ } => match op {
            Op::Eq | Op::Neq | Op::Lt | Op::Le | Op::Gt | Op::Ge | Op::And | Op::Or => Type::Bool,
        },
        Expr::Function(function) => match function.name {
            FunctionName::Min => Type::Int,
            FunctionName::Max => Type::Int,
            FunctionName::Sum => Type::Int,
            FunctionName::Avg => Type::Int,
            FunctionName::Count => Type::Int,
            FunctionName::Contains => Type::Bool,
            FunctionName::Concat => Type::Varchar,
        },

        Expr::SubQuery(_) => todo!(),
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard(_) => todo!(),
    }
}
