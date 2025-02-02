use super::{write_iter, LogicalPlan, LogicalPlanError, LogicalPlanError::*};
use crate::catalog::schema::{Schema, SchemaBuilder};
use crate::column;
use crate::sql::{Expr, Literal};

pub struct Values {
    schema: Schema,
    values: Vec<Vec<Expr>>,
    alias: Option<String>,
}

impl std::fmt::Display for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const LIMIT: usize = 3;

        write!(f, "Values [")?;
        let mut tmp = "";
        for values in self.values.iter().take(LIMIT) {
            write!(f, "{tmp}")?;
            write!(f, "(")?;
            write_iter(f, &mut values.iter(), ", ")?;
            write!(f, ")")?;
            tmp = ", ";
        }

        if self.values.len() > LIMIT {
            write!(f, ", ... ")?;
        }

        write!(f, "]")
    }
}

impl From<Values> for LogicalPlan {
    fn from(values: Values) -> Self {
        Self::Values(values)
    }
}

impl Values {
    pub fn new(values: Vec<Vec<Expr>>) -> Result<Self, LogicalPlanError> {
        let schema = infer_schema(&values)?;

        Ok(Self { schema, values, alias: None })
    }

    pub fn new_with_alias(values: Vec<Vec<Expr>>, alias: String) -> Result<Self, LogicalPlanError> {
        let mut values = Self::new(values)?;
        values.alias = Some(alias);
        Ok(values)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }
}

fn infer_schema(values: &Vec<Vec<Expr>>) -> Result<Schema, LogicalPlanError> {
    let mut schema = SchemaBuilder::new();

    if values.is_empty() {
        return Ok(schema.build());
    }

    let mut pos = 0;
    for expr in &values[0] {
        match expr {
            Expr::Ident(..) => Err(NotImplemented("references aren't supported inside VALUES"))?,
            Expr::Literal(literal) => {
                let column = match literal {
                    Literal::Number(_) => column!(format!("c{pos}"), Int),
                    Literal::String(_) => column!(format!("c{pos}"), Varchar),
                    Literal::Bool(_) => column!(format!("c{pos}"), Bool),
                    Literal::Decimal(_) | Literal::Null => todo!(),
                };
                schema.append(column);
            }
            Expr::IsNull { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::BinaryOp { .. } => {
                let column = column!(format!("c{pos}"), Bool);
                schema.append(column);
            }

            Expr::SubQuery(..) => todo!(),
            Expr::Function(..) => todo!(),
            Expr::Wildcard => todo!(),
            Expr::QualifiedWildcard(..) => todo!(),
        }

        pos += 1;
    }

    Ok(schema.build())
}
