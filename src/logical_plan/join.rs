use super::{write_iter, LogicalPlan, LogicalPlanError, LogicalPlanError::*};
use crate::catalog::schema::Schema;
use crate::sql::{Expr, Ident};

pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
}

pub struct Join {
    pub constraint: JoinConstraint,
    pub schema: Schema,
    pub left_input: Box<LogicalPlan>,
    pub right_input: Box<LogicalPlan>,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Join ")?;
        match &self.constraint {
            JoinConstraint::On(expr) => write!(f, "ON {}", expr),
            JoinConstraint::Using(columns) => {
                write!(f, "USING (")?;
                write_iter(f, &mut columns.iter(), ", ")?;
                write!(f, ")")
            }
        }
    }
}

impl From<Join> for LogicalPlan {
    fn from(join: Join) -> Self {
        Self::Join(join)
    }
}

impl Join {
    fn new(constraint: JoinConstraint, left_input: LogicalPlan, right_input: LogicalPlan) -> Self {
        let left_input = Box::new(left_input);
        let right_input = Box::new(right_input);
        let schema = left_input.schema().join(right_input.schema());
        Self { constraint, schema, left_input, right_input }
    }

    pub fn on(
        expr: Expr,
        left_input: impl Into<LogicalPlan>,
        right_input: impl Into<LogicalPlan>,
    ) -> Result<Self, LogicalPlanError> {
        let left_input = left_input.into();
        let right_input = right_input.into();

        // TODO: validate expr

        Ok(Self::new(JoinConstraint::On(expr), left_input, right_input))
    }

    pub fn using(
        columns: Vec<Ident>,
        left_input: impl Into<LogicalPlan>,
        right_input: impl Into<LogicalPlan>,
    ) -> Result<Self, LogicalPlanError> {
        let left_input = left_input.into();
        let right_input = right_input.into();
        for column in &columns {
            // Assuming the identifiers only have one part
            if left_input.schema().find_column_by_name(&column[0]).is_none() {
                Err(UnknownColumn(column.to_string()))?
            };

            if right_input.schema().find_column_by_name(&column[0]).is_none() {
                Err(UnknownColumn(column.to_string()))?
            };

            // TODO: using columns aren't qualified so `schema.join()` should be updated or a new method
            // created
        }

        Ok(Self::new(JoinConstraint::Using(columns), left_input, right_input))
    }
}
