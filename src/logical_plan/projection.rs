use crate::{
    catalog::schema::{Column, Schema, SchemaBuilder, Type},
    column,
    logical_plan::{expr_type, write_iter, LogicalOperator, LogicalOperatorError},
    sql::{Expr, Ident, SelectItem},
};

/// `schema`, `projection` have the same length, each field has a corresponding field at the
/// matching index
/// `input_idents` has the same length as the number of idents in the input schema and is in the
/// same order
#[derive(Debug)]
pub struct ProjectionAttributes {
    schema: Schema,
    projection: Vec<SelectItem>,
    input_idents: Vec<(Type, usize)>,
}

impl ProjectionAttributes {
    fn new(
        projection: Vec<SelectItem>,
        input_schema: &Schema,
    ) -> Result<Self, LogicalOperatorError> {
        let mut input_idents = Vec::new();
        let mut schema = SchemaBuilder::new();
        for item in &projection {
            match item {
                SelectItem::Expr(Expr::Ident(ident @ Ident::Single(column))) => {
                    let column = input_schema
                        .find_column_by_name(column)
                        .cloned()
                        .ok_or(format!("unknown column: {ident}"))?;
                    input_idents.push((column.ty, column.offset));
                    schema.append(column)
                }
                SelectItem::Expr(Expr::Ident(ident @ Ident::Compound(idents))) => {
                    let column = input_schema
                        .find_column_by_name_and_table(&idents[0], &idents[1])
                        .cloned()
                        .ok_or(format!("unknown column: {ident}"))?;
                    input_idents.push((column.ty, column.offset));
                    schema.append(column)
                }
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
                            table.as_ref().is_some_and(|table| table.as_str() == &ident[0])
                        })
                        .cloned(),
                ),
            };
        }

        let schema = schema.build();

        Ok(Self { schema, projection, input_idents })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }

    pub fn input_idents(&self) -> &Vec<(Type, usize)> {
        &self.input_idents
    }

    pub fn projection(&self) -> &Vec<SelectItem> {
        &self.projection
    }
}

pub struct Projection {
    pub input: Box<LogicalOperator>,
    pub attributes: ProjectionAttributes,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Projection [")?;
        write_iter(f, &mut self.attributes.projection.iter(), ", ")?;
        write!(f, "]")
    }
}

impl From<Projection> for LogicalOperator {
    fn from(projection: Projection) -> Self {
        Self::Projection(projection)
    }
}

impl Projection {
    pub fn new(
        projection: Vec<SelectItem>,
        input: impl Into<LogicalOperator>,
    ) -> Result<Self, LogicalOperatorError> {
        let input = Box::new(input.into());
        let attributes = ProjectionAttributes::new(projection, input.schema())?;

        Ok(Self { input, attributes })
    }
}
