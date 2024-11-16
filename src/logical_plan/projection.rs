use {
    super::{write_iter, Expr, FunctionName, LogicalPlan, LogicalPlanError, LogicalPlanError::*},
    crate::catalog::{Column, Schema, Type},
};

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
        // TODO: validate identifiers inside expressions
        let schema = exprs
            .iter()
            .map(|expr| match expr {
                Expr::Ident(ident) => match input.schema().find(ident) {
                    Some(Column { name, ty, .. }) => Ok((name.clone(), *ty)),
                    None => Err(InvalidIdent(ident.clone())),
                },
                expr @ Expr::Value(value) => Ok((expr.to_string(), value.into())),
                expr @ Expr::IsNull(_)
                | expr @ Expr::IsNotNull(_)
                | expr @ Expr::InList { .. }
                | expr @ Expr::Between { .. }
                | expr @ Expr::BinaryOp { .. } => Ok((expr.to_string(), Type::Bool)),
                Expr::Function(function) => match function.name {
                    FunctionName::Min
                    | FunctionName::Max
                    | FunctionName::Sum
                    | FunctionName::Avg
                    | FunctionName::Count => Ok((function.to_string(), Type::Int)),
                    FunctionName::Contains => Ok((function.to_string(), Type::Bool)),
                    FunctionName::Concat => Ok((function.to_string(), Type::Varchar)),
                },
            })
            .collect::<Result<Vec<(String, Type)>, LogicalPlanError>>()?
            .into();

        Ok(Self { schema, input })
    }
}
