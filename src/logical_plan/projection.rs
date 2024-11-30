use super::{write_iter, LogicalPlan, LogicalPlanError, LogicalPlanError::*};
use crate::catalog::{Column, Schema, Type};
use crate::sql::{Expr, FunctionName};

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
    pub fn new(exprs: &[Expr], input: impl Into<LogicalPlan>) -> Result<Self, LogicalPlanError> {
        let input = Box::new(input.into());

        // TODO: use aliases from the parser
        let schema = exprs
            .iter()
            .map(|expr| validate_expr(expr, input.schema()))
            .collect::<Result<Vec<_>, _>>()?
            .into();

        Ok(Self { schema, input })
    }
}

fn validate_expr(expr: &Expr, schema: &Schema) -> Result<(String, Type), LogicalPlanError> {
    let column = match expr {
        Expr::Ident(ident) => match schema.find(ident.to_string().as_str()) {
            Some(Column { name, ty, .. }) => (name.clone(), *ty),
            None => Err(InvalidIdent(ident.clone()))?,
        },
        Expr::Literal(literal) => (expr.to_string(), literal.into()),
        Expr::IsNull { expr: e, negated: _ } => {
            let _ = validate_expr(e, schema)?;
            (expr.to_string(), Type::Bool)
        }
        Expr::InList { expr: e, list, .. } => {
            list.iter().map(|e| validate_expr(e, schema)).collect::<Result<Vec<_>, _>>()?;
            validate_expr(e, schema)?;
            (expr.to_string(), Type::Bool)
        }
        Expr::Between { expr: e, low, high, .. } => {
            validate_expr(e, schema)?;
            validate_expr(low, schema)?;
            validate_expr(high, schema)?;
            (expr.to_string(), Type::Bool)
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left, schema)?;
            validate_expr(right, schema)?;
            (expr.to_string(), Type::Bool)
        }
        Expr::Function(function) => {
            function
                .args
                .iter()
                .map(|expr| validate_expr(expr, &schema))
                .collect::<Result<Vec<_>, _>>()?;

            match function.name {
                FunctionName::Min
                | FunctionName::Max
                | FunctionName::Sum
                | FunctionName::Avg
                | FunctionName::Count => (function.to_string(), Type::Int),
                FunctionName::Contains => (function.to_string(), Type::Bool),
                FunctionName::Concat => (function.to_string(), Type::Varchar),
            }
        }
        Expr::SubQuery(_) => todo!(),
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard(_) => todo!(),
    };

    Ok(column)
}
