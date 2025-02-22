use crate::catalog::Catalog;
use crate::logical_plan::{
    scan, scan_with_alias, values, values_with_alias, Builder as LogicalOperatorBuilder,
    LogicalOperator, LogicalOperatorError,
};
use crate::sql::{
    FromTable, Ident, Insert, InsertInput, Join, JoinConstraint, JoinType, OrderByExpr, Query,
    Select, Statement,
};

#[derive(PartialEq)]
pub struct PlannerError(String);
impl std::error::Error for PlannerError {}

impl std::fmt::Display for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "planner error: {}", self.0)
    }
}

impl std::fmt::Debug for PlannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<String> for PlannerError {
    fn from(value: String) -> Self {
        PlannerError(value)
    }
}

impl From<&str> for PlannerError {
    fn from(value: &str) -> Self {
        PlannerError(value.into())
    }
}

impl From<LogicalOperatorError> for PlannerError {
    fn from(value: LogicalOperatorError) -> Self {
        value.to_string().into()
    }
}

pub struct Planner {
    catalog: Catalog,
}

impl Planner {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    pub fn plan_statement(&self, statement: Statement) -> Result<LogicalOperator, PlannerError> {
        let statement = match statement {
            Statement::Select(select) => self.build_select(select)?,
            Statement::Insert(insert) => self.build_insert(insert)?,
            Statement::Update(_) => todo!(),
            Statement::Delete(_) => todo!(),
            Statement::Create(_) => todo!(),
        };

        Ok(statement.build())
    }

    fn build_select(&self, select: Select) -> Result<LogicalOperatorBuilder, PlannerError> {
        let Select { body, order, limit } = select;

        let mut query = self.build_query(body)?;

        query = match order {
            Some(OrderByExpr { exprs, desc: false }) => query.sort(exprs),
            Some(OrderByExpr { exprs, desc: true }) => query.sort_desc(exprs),
            None => query,
        };

        if let Some(expr) = limit {
            query = query.limit(expr)?;
        }

        Ok(query)
    }

    fn build_query(
        &self,
        Query { projection, from, joins, filter, group }: Query,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let (mut query, _) = self.build_from(from)?;

        for join in joins {
            let Join { from, ty, constraint } = join;
            let (rhs, _) = self.build_from(from)?;

            let JoinType::Inner = ty;

            query = match constraint {
                JoinConstraint::On(expr) => query.join_on(rhs.build(), expr)?,
                JoinConstraint::Using(columns) => query.join_using(rhs.build(), columns)?,
            };

            // query = query.join_on(rhs.build(), predicate);
        }

        if let Some(filter) = filter {
            // This filter might reference some aliased columns in the projection, we may need to
            // build the projection schema first and replace any aliases with table.column
            // references.
            query = query.filter(filter);
        }

        // There may or may not be a aggregate function in the projection. If there isn't, then it
        // should still group by, where the first processed tuple columns are in the result
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
    ) -> Result<(LogicalOperatorBuilder, String), PlannerError> {
        let builder = match from {
            FromTable::Table { name, alias } => {
                let Ident::Single(name) = name else {
                    Err(format!("multiple schemas aren't supported: {name}"))?
                };
                let table_info = self
                    .catalog
                    .get_table_by_name(&name)
                    .ok_or(format!("unknown table: {name}"))?;

                if let Some(alias) = alias {
                    // Alias is applied at the `Scan` node, single table
                    (scan_with_alias(table_info, alias.clone()), alias)
                } else {
                    (scan(table_info), name)
                }
            }
            FromTable::Derived { query, alias } => {
                let mut query = self.build_query(*query)?;

                // Alias applies to all columns in the query, all tables
                let Some(alias) = alias else { Err(format!("internal: expected derived alias"))? };
                query.schema_mut().qualify(&alias);

                (query, alias)
            }
            FromTable::Values { rows, alias } => {
                if let Some(alias) = alias {
                    (values_with_alias(rows, alias.clone())?, alias)
                } else {
                    (values(rows)?, "".into())
                }
            }
        };

        Ok(builder)
    }

    fn build_insert(
        &self,
        Insert { table, input }: Insert,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let Ident::Single(name) = table else {
            Err(format!("multiple schemas aren't supported: {table}"))?
        };
        let table_info =
            self.catalog.get_table_by_name(&name).ok_or(format!("unknown table: {name}"))?;

        let builder = match input {
            InsertInput::Values(rows) => values(rows)?.insert(table_info)?,
            InsertInput::Query(query) => self.build_query(query)?.insert(table_info)?,
        };

        Ok(builder)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::Catalog;
    use crate::disk::Memory;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::planner::Planner;
    use crate::replacer::LRU;
    use crate::sql::Parser;
    use crate::{column, schema};

    macro_rules! test_statement {
        ($name:ident, {$( $table:expr => $columns:expr )+}, $statement:expr, $want:expr) => {
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

                let statement= $statement;
                let mut parser = Parser::new(&statement).unwrap();
                let select = parser.parse_statements().unwrap().pop().unwrap();
                let planner = Planner::new(catalog);
                let plan = planner.plan_statement(select).unwrap();

                assert_eq!(plan.to_string(), $want);
            }
        };

        ($name:ident, $statement:expr, $want:expr) => {
            #[test]
            fn $name() {
                const MEMORY: usize = PAGE_SIZE * 3;
                const K: usize = 2;
                let disk = Memory::new::<MEMORY>();
                let replacer = LRU::new(K);
                let pc = PageCache::new(disk, replacer, 0);

                let catalog = Catalog::new(pc);

                let statement = $statement;
                let mut parser = Parser::new(&statement).unwrap();
                let select = parser.parse_statements().unwrap().pop().unwrap();
                let planner = Planner::new(catalog);
                let plan = planner.plan_statement(select).unwrap();

                assert_eq!(plan.to_string(), $want);
            }
        };
    }

    test_statement!(
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

    test_statement!(
        t2,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
            "t2" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
        },
        "SELECT * FROM t1 JOIN t2 ON (t1.c1 = t2.c1) where t1.c1 > 5",
        "\
Projection [*]
    Filter [t1.c1 > 5]
        Join ON t1.c1 = t2.c1
            Scan table=t1 alias= oid=0
            Scan table=t2 alias= oid=1
"
    );

    test_statement!(
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
        Join USING (c1, c4)
            Join USING (c2, c3)
                Scan table=t1 alias= oid=0
                Scan table=t2 alias= oid=1
            Scan table=t3 alias= oid=2
"
    );

    test_statement!(
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

    test_statement!(
        t5,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) }
            "t2" => schema! { column!("c2", Int), column!("c3", Varchar), column!("c4", BigInt) }
        },
        "SELECT d1.*, d2.c3, d2.c4 FROM (SELECT * FROM t1 WHERE c1 IN (1, 2, 3)) d1
        JOIN (SELECT c2, c3, c4 FROM t2 WHERE c2 != '') d2 USING (c2)",
        "\
Projection [d1.*, d2.c3, d2.c4]
    Join USING (c2)
        Projection [*]
            Filter [c1 IN (1, 2, 3)]
                Scan table=t1 alias= oid=0
        Projection [c2, c3, c4]
            Filter [c2 != '']
                Scan table=t2 alias= oid=1
"
    );

    test_statement!(
        t6,
        "SELECT * FROM VALUES (1, 2, 3), (4, 5, 6)",
        "\
Projection [*]
    Values [(1, 2, 3), (4, 5, 6)]
"
    );

    test_statement!(
        t7,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Int), column!("c3", Int) }
        },
        "INSERT INTO t1 VALUES (1, 2, 3), (4, 5, 6)",
        "\
Insert table=t1 oid=0
    Values [(1, 2, 3), (4, 5, 6)]
"
    );

    test_statement!(
        t8,
        {
            "t1" => schema! { column!("c1", Int), column!("c2", Int), column!("c3", Int) }
        },
        "INSERT INTO t1 (SELECT * FROM VALUES (1, 2, 3), (4, 5, 6))",
        "\
Insert table=t1 oid=0
    Projection [*]
        Values [(1, 2, 3), (4, 5, 6)]
"
    );
}
