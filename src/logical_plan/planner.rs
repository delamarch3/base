use crate::catalog::Catalog;
use crate::logical_plan::{scan, scan_with_alias};
use crate::sql::{
    Expr, FromTable, Ident, Join, JoinConstraint, JoinType, OrderByExpr, Query, Select, Statement,
};

use super::{Builder as LogicalPlanBuilder, LogicalPlan, LogicalPlanError, LogicalPlanError::*};

pub struct Planner {
    catalog: Catalog,
}

impl Planner {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    pub fn plan_statement(&self, statement: Statement) -> Result<LogicalPlan, LogicalPlanError> {
        let statement = match statement {
            Statement::Select(select) => self.build_select(select)?,
            Statement::Insert(_) => todo!(),
            Statement::Update(_) => todo!(),
            Statement::Delete(_) => todo!(),
            Statement::Create(_) => todo!(),
        };

        Ok(statement.build())
    }

    fn build_select(&self, select: Select) -> Result<LogicalPlanBuilder, LogicalPlanError> {
        let Select { body, order, limit } = select;

        let mut query = self.build_query(body)?;

        query = match order {
            Some(OrderByExpr { exprs, desc: false }) => query.sort(exprs),
            Some(OrderByExpr { exprs, desc: true }) => query.sort_desc(exprs),
            None => query,
        };

        if let Some(expr) = limit {
            query = query.limit(expr)
        }

        Ok(query)
    }

    fn build_query(&self, query: Query) -> Result<LogicalPlanBuilder, LogicalPlanError> {
        let Query { projection, from, joins, filter, group } = query;

        let (mut query, _) = self.build_from(from)?;

        for join in joins {
            let Join { from, ty, constraint } = join;
            let (rhs, rhs_table) = self.build_from(from)?;

            let JoinType::Inner = ty;

            // USING verifies the columns but ON doesn't. This should be made consistent
            let schema = query.schema();
            let predicate = match constraint {
                JoinConstraint::On(expr) => expr,
                JoinConstraint::Using(join_columns) => {
                    let mut predicate: Option<Expr> = None;
                    for join_column in join_columns {
                        let Some(column) = schema.find_column_by_name(&join_column[0]) else {
                            Err(UnknownColumn(join_column.to_string()))?
                        };

                        let rhs_column = join_column.qualify(&rhs_table);
                        if rhs.schema().find_column_by_name(&rhs_column[1]).is_none() {
                            Err(UnknownColumn(rhs_column.to_string()))?
                        }

                        let Some(table) = &column.table else { Err(Internal)? };
                        let lhs_column =
                            Expr::Ident(Ident::Compound(vec![table.clone(), column.name.clone()]));
                        let expr = Expr::Ident(rhs_column).eq(lhs_column);
                        predicate = Some(predicate.map_or(expr.clone(), |p| p.and(expr)));
                    }

                    predicate.unwrap()
                }
            };

            query = query.join(rhs.build(), predicate);
        }

        if let Some(filter) = filter {
            // This filter might reference some aliased columns in the projection, we may need to
            // build the projection schema first and replace any aliases with table.column
            // references.
            query = query.filter(filter);
        }

        // There may or may not be a aggregate function in the projection. If there isn't, then it
        // should still group by, where the last processed tuple columns are in the result
        if group.len() > 0 {
            todo!()
        }

        // The projection may have some aggregate functions. If they exist then the projection is
        // followed by a aggregate step.
        query = query.project(projection)?;

        Ok(query)
    }

    /// Creates a `Scan` operator and returns it along with the table alias
    fn build_from(
        &self,
        from: FromTable,
    ) -> Result<(LogicalPlanBuilder, String), LogicalPlanError> {
        match from {
            FromTable::Table { name, alias } => {
                let Ident::Single(name) = name else { Err(NotImplemented("multiple schema"))? };
                let table_info =
                    self.catalog.get_table_by_name(&name).ok_or(UnknownTable(name.clone()))?;

                if let Some(alias) = alias {
                    // Alias is applied at the `Scan` node, single table
                    Ok((scan_with_alias(table_info, alias.clone()), alias))
                } else {
                    Ok((scan(table_info), name))
                }
            }
            FromTable::Derived { query, alias } => {
                let mut query = self.build_query(*query)?;

                // Alias applies to all columns in the query, all tables
                let Some(alias) = alias else { Err(Internal)? };
                query.schema_mut().qualify(&alias);

                Ok((query, alias))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::Catalog;
    use crate::disk::Memory;
    use crate::logical_plan::planner::Planner;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::sql::Parser;
    use crate::{column, schema};

    macro_rules! test_plan_select {
        ($name:ident, {$( $table:expr => $columns:expr )+}, $query:expr, $want:expr) => {
            #[test]
            fn $name() {
                const MEMORY: usize = PAGE_SIZE * 3;
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
            "t1" => schema!{ column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
        },
        "SELECT * FROM t1 WHERE c1 = c2",
        "\
Projection [*]
    Filter [c1 = c2]
        Scan table=t1 alias= oid=0
"
    );

    test_plan_select!(
        t2,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
            "t2" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
        },
        "SELECT * FROM t1 JOIN t2 ON (t1.c1 = t2.c1) where t1.c1 > 5",
        "\
Projection [*]
    Filter [t1.c1 > 5]
        HashJoin [t1.c1 = t2.c1]
            Scan table=t1 alias= oid=0
            Scan table=t2 alias= oid=1
"
    );

    test_plan_select!(
        t3,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
            "t2" => schema! { column!("c2", Int), column!("c3", Varchar), column!("c4", BigInt) }
            "t3" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c4", BigInt) }
        },
        "SELECT * FROM t1 JOIN t2 USING (c2, c3) JOIN t3 USING (c1, c4) where c1 > 5",
        "\
Projection [*]
    Filter [c1 > 5]
        HashJoin [t3.c1 = t1.c1 AND t3.c4 = t2.c4]
            HashJoin [t2.c2 = t1.c2 AND t2.c3 = t1.c3]
                Scan table=t1 alias= oid=0
                Scan table=t2 alias= oid=1
            Scan table=t3 alias= oid=2
"
    );

    test_plan_select!(
        t4,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt),
                     column!("c4", BigInt), column!("c5", BigInt)}
        },
        "SELECT c1, c2, c3, c4 AS column_four FROM t1 WHERE c5 = '' AND column_four > 10",
        "\
Projection [c1, c2, c3, c4 AS column_four]
    Filter [c5 = '' AND column_four > 10]
        Scan table=t1 alias= oid=0
"
    );

    test_plan_select!(
        t5,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
            "t2" => schema! { column!("c2", Int), column!("c3", Varchar), column!("c4", BigInt) }
        },
        "SELECT d1.*, d2.c3, d2.c4 FROM (SELECT * FROM t1 WHERE c1 IN (1, 2, 3)) d1
        JOIN (SELECT c2, c3, c4 FROM t2 WHERE c2 != '') d2 USING(c2)",
        "\
Projection [d1.*, d2.c3, d2.c4]
    HashJoin [d2.c2 = d1.c2]
        Projection [*]
            Filter [c1 IN (1, 2, 3)]
                Scan table=t1 alias= oid=0
        Projection [c2, c3, c4]
            Filter [c2 != '']
                Scan table=t2 alias= oid=1
"
    );
}
