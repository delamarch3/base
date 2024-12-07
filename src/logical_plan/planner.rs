use crate::{
    catalog::Catalog,
    logical_plan::scan,
    sql::{
        Expr, FromTable, Ident, Join, JoinConstraint, JoinType, OrderByExpr, Query, Select,
        Statement,
    },
};

use super::{Builder as LogicalPlanBuilder, LogicalPlan};

pub enum PlannerError {
    NotImplemented(String),
    UnknownTable(String),
}
use PlannerError::*;

pub struct Planner {
    catalog: Catalog,
}

impl Planner {
    pub fn plan_statement(&self, statement: Statement) -> Result<LogicalPlan, PlannerError> {
        let statement = match statement {
            Statement::Select(select) => self.build_select(select)?,
            Statement::Insert(_) => todo!(),
            Statement::Update(_) => todo!(),
            Statement::Delete(_) => todo!(),
            Statement::Create(_) => todo!(),
        };

        Ok(statement.build())
    }

    fn build_select(&self, select: Select) -> Result<LogicalPlanBuilder, PlannerError> {
        let Select { body, order, limit } = select;

        let mut query = self.build_query(body)?;

        if let Some(OrderByExpr { exprs, desc }) = order {
            query = query.order_by(&exprs, desc)
        }

        if let Some(expr) = limit {
            query = query.limit(expr)
        }

        Ok(query)
    }

    fn build_query(&self, query: Query) -> Result<LogicalPlanBuilder, PlannerError> {
        let Query { projection, from, joins, filter, group } = query;

        let mut query = self.build_from(from)?;

        for join in joins {
            let Join { from, ty, constraint } = join;
            let rhs = self.build_from(from)?;

            let JoinType::Inner = ty;

            let predicate = match constraint {
                JoinConstraint::On(expr) => expr,
                JoinConstraint::Using(_) => todo!(),
            };

            query = query.join(rhs.build(), predicate);
        }

        if let Some(filter) = filter {
            query = query.filter(filter);
        }

        if group.len() > 0 {
            todo!()
        }

        query = query.project(&projection);

        Ok(query)
    }

    fn build_from(&self, from: FromTable) -> Result<LogicalPlanBuilder, PlannerError> {
        let FromTable::Table { name, alias } = from else {
            Err(NotImplemented("derived tables are not implemented yet".into()))?
        };

        let Ident::Single(name) = name else {
            Err(NotImplemented("multiple schema is not implemented yet".into()))?
        };

        let table_info = self
            .catalog
            .get_table_by_name(&name)
            .ok_or(UnknownTable(format!("unknown table `{name}`")))?;

        Ok(scan(&table_info))
    }
}
