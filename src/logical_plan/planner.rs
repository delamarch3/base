use std::collections::HashSet;

use crate::catalog::Catalog;
use crate::disk::Disk;
use crate::logical_plan::scan;
use crate::sql::{
    Expr, FromTable, Ident, Join, JoinConstraint, JoinType, OrderByExpr, Query, Select, Statement,
};

use super::{Builder as LogicalPlanBuilder, LogicalPlan};

#[derive(Debug)]
pub enum PlannerError {
    NotImplemented(String),
    UnknownTable(String),
}
use PlannerError::*;

pub struct Planner<D: Disk> {
    catalog: Catalog<D>,
}

impl<D: Disk> Planner<D> {
    pub fn new(catalog: Catalog<D>) -> Self {
        Self { catalog }
    }

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
            query = query.filter(filter)
        }

        if group.len() > 0 {
            todo!()
        }

        query = query.project(projection);

        Ok(query)
    }

    fn build_from(&self, from: FromTable) -> Result<LogicalPlanBuilder, PlannerError> {
        let FromTable::Table { name, alias } = from else {
            Err(NotImplemented("derived tables are not implemented yet".into()))?
        };

        let Ident::Single(name) = name else {
            Err(NotImplemented("multiple schema is not implemented yet".into()))?
        };

        let table_info =
            self.catalog.get_table_by_name(&name).ok_or(UnknownTable(format!("`{name}`")))?;

        Ok(scan(&table_info))
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{Catalog, Type};
    use crate::disk::Memory;
    use crate::logical_plan::planner::Planner;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::sql::Parser;

    macro_rules! test_plan_select {
        ($name:ident, {$( $table:expr => $columns:expr )+}, $query:expr, $want:expr) => {
            #[test]
            fn $name() {
                const MEMORY: usize = PAGE_SIZE * 2;
                const K: usize = 2;
                let disk = Memory::new::<MEMORY>();
                let replacer = LRU::new(K);
                let pc = PageCache::new(disk, replacer, 0);

                let mut catalog = Catalog::new(pc);

                $(
                catalog
                    .create_table($table, $columns)
                    .unwrap();
                )+

                let query = $query;
                let mut parser = Parser::new(&query).unwrap();
                let select = parser.parse_statements().unwrap().pop().unwrap();
                let planner = Planner::new(catalog);
                let plan = planner.plan_statement(select).unwrap();

                assert_eq!(plan.to_string(), $want);
            }
        };
    }

    test_plan_select!(
        t1,
        {
            "t1" => [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)]
        },
        "SELECT * FROM t1 WHERE c1 = c2",
        "\
Projection [*]
    Filter [c1 = c2]
        Scan t1 0
"
    );

    test_plan_select!(
        t2,
        {
            "t1" => [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)]
            "t2" => [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)]
        },
        "SELECT * FROM t1 JOIN t2 ON (t1.c1 = t2.c1) where t1.c1 > 5",
        "\
Projection [*]
    Filter [t1.c1 > 5]
        NestedLoopJoin [t1.c1 = t2.c1]
            Scan t1 0
            Scan t2 1
"
    );

    test_plan_select!(
        t3,
        {
            "t1" => [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt),
                     ("c4", Type::BigInt), ("c5", Type::BigInt)]
        },
        "SELECT c1, c2, c3, c4 AS column_four FROM t1 WHERE c5 = '' AND column_four > 10",
        "\
Projection [c1, c2, c3, c4 AS column_four]
    Filter [c5 = \"\" AND column_four > 10]
        Scan t1 0
"
    );
}
