use std::sync::MutexGuard;

use crate::{
    catalog::{schema::SchemaBuilder, Catalog, SharedCatalog},
    column,
    logical_plan::{
        create, explain, scan, scan_with_alias, values, values_with_alias,
        Builder as LogicalOperatorBuilder, LogicalOperator, LogicalOperatorError,
    },
    sql::{
        ColumnDef, ColumnType, Create, Explain, FromTable, Ident, Insert, InsertInput, Join,
        JoinConstraint, JoinType, OrderByExpr, Query, Select, Statement,
    },
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
    catalog: SharedCatalog,
}

impl Planner {
    pub fn new(catalog: SharedCatalog) -> Self {
        Self { catalog }
    }

    pub fn plan(&self, statement: Statement) -> Result<LogicalOperator, PlannerError> {
        let catalog = self.catalog.lock().unwrap();

        self.plan_statement(&catalog, statement)
    }

    fn plan_statement(
        &self,
        catalog: &MutexGuard<'_, Catalog>,
        statement: Statement,
    ) -> Result<LogicalOperator, PlannerError> {
        let statement = match statement {
            Statement::Select(select) => self.build_select(&catalog, select)?,
            Statement::Insert(insert) => self.build_insert(&catalog, insert)?,
            Statement::Update(_) => todo!(),
            Statement::Delete(_) => todo!(),
            Statement::Create(create) => self.build_create(&catalog, create)?,
            Statement::Explain(explain) => self.build_explain(&catalog, explain)?,
        };

        Ok(statement.build())
    }

    fn build_select(
        &self,
        catalog: &MutexGuard<'_, Catalog>,
        select: Select,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let Select { body, order, limit } = select;

        let mut query = self.build_query(catalog, body)?;

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
        catalog: &MutexGuard<'_, Catalog>,
        Query { projection, from, joins, filter, group }: Query,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let (mut query, _) = self.build_from(catalog, from)?;

        for join in joins {
            let Join { from, ty, constraint } = join;
            let (rhs, _) = self.build_from(catalog, from)?;

            let JoinType::Inner = ty;

            query = match constraint {
                JoinConstraint::On(expr) => query.join_on(rhs.build(), expr)?,
                JoinConstraint::Using(columns) => query.join_using(rhs.build(), columns)?,
            };
        }

        if let Some(filter) = filter {
            // This filter might reference some aliased columns in the projection, we may need to
            // build the projection schema first and replace any aliases with table.column
            // references.
            query = query.filter(filter);
        }

        // There may or may not be an aggregate function in the projection. If there isn't, then it
        // should still group by, where the first/last processed tuple columns are in the result
        if !group.is_empty() {
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
        catalog: &MutexGuard<'_, Catalog>,
        from: FromTable,
    ) -> Result<(LogicalOperatorBuilder, String), PlannerError> {
        let builder = match from {
            FromTable::Table { name, alias } => {
                let Ident::Single(name) = name else {
                    Err(format!("multiple schemas aren't supported: {name}"))?
                };
                let table_info =
                    catalog.get_table_by_name(&name).ok_or(format!("unknown table: {name}"))?;

                if let Some(alias) = alias {
                    // Alias is applied at the `Scan` node, single table
                    (scan_with_alias(table_info, alias.clone()), alias)
                } else {
                    (scan(table_info), name)
                }
            }
            FromTable::Derived { query, alias } => {
                let mut query = self.build_query(catalog, *query)?;

                // Alias applies to all columns in the query, all tables
                let Some(alias) = alias else {
                    Err("internal: expected derived alias".to_string())?
                };
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
        catalog: &MutexGuard<'_, Catalog>,
        Insert { table, input }: Insert,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let Ident::Single(name) = table else {
            Err(format!("multiple schemas aren't supported: {table}"))?
        };
        let table_info =
            catalog.get_table_by_name(&name).ok_or(format!("unknown table: {name}"))?;

        let builder = match input {
            InsertInput::Values(rows) => values(rows)?.insert(table_info)?,
            InsertInput::Query(query) => self.build_query(catalog, query)?.insert(table_info)?,
        };

        Ok(builder)
    }

    fn build_create(
        &self,
        _catalog: &MutexGuard<'_, Catalog>,
        Create { name, columns }: Create,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let Ident::Single(name) = name else {
            Err(format!("multiple schemas aren't supported: {name}"))?
        };

        // We'll invoke the catalog methods in the `Create` physical operator

        let mut builder = SchemaBuilder::new();
        for ColumnDef { ty, name } in columns {
            let column = match ty {
                ColumnType::Int => column!(name, Int),
                ColumnType::Varchar => column!(name, Varchar),
            };

            builder.append(column);
        }

        let schema = builder.build();

        Ok(create(name, schema))
    }

    fn build_explain(
        &self,
        catalog: &MutexGuard<'_, Catalog>,
        Explain { statement }: Explain,
    ) -> Result<LogicalOperatorBuilder, PlannerError> {
        let plan = self.plan_statement(catalog, *statement)?;

        Ok(explain(plan))
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};

    use crate::{
        catalog::Catalog, disk::Memory, page::PAGE_SIZE, page_cache::PageCache, planner::Planner,
        replacer::LRU, schema, sql::Parser,
    };

    macro_rules! test_statement {
        ($name:ident, {$( $table:expr => $columns:expr )+}, $statement:expr, $want:expr) => {
            #[test]
            fn $name() {
                const MEMORY: usize = PAGE_SIZE * 3;
                const K: usize = 2;
                let disk = Memory::new::<MEMORY>();
                let replacer = LRU::new(K);
                let pc = PageCache::new(disk, replacer, 0);

                let shared_catalog = Arc::new(Mutex::new(Catalog::new(pc)));

                {
                    let mut catalog = shared_catalog.lock().unwrap();
                    $(
                    catalog
                        .create_table($table, $columns)
                        .unwrap();
                    )+
                }

                let statement= $statement;
                let mut parser = Parser::new(&statement).unwrap();
                let select = parser.parse_statements().unwrap().pop().unwrap();
                let planner = Planner::new(shared_catalog);
                let plan = planner.plan(select).unwrap();

                assert_eq!($want, plan.to_string());
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

                let shared_catalog = Arc::new(Mutex::new(Catalog::new(pc)));

                let statement = $statement;
                let mut parser = Parser::new(&statement).unwrap();
                let select = parser.parse_statements().unwrap().pop().unwrap();
                let planner = Planner::new(shared_catalog);
                let plan = planner.plan(select).unwrap();

                assert_eq!($want, plan.to_string());
            }
        };
    }

    test_statement!(
        simple_select,
        {
            "t1" => schema!{ c1 Int, c2 Varchar, c3 BigInt }
        },
        "SELECT * FROM t1 WHERE c1 = c2",
        "\
Projection [*]
    Filter [c1 = c2]
        Scan table=t1 alias= oid=0
"
    );

    test_statement!(
        select_with_join_on,
        {
            "t1" => schema! { c1 Int, c2 Varchar, c3 BigInt }
            "t2" => schema! { c1 Int, c2 Varchar, c3 BigInt }
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
        select_with_join_using,
        {
            "t1" => schema! { c1 Int, c2 Varchar, c3 BigInt }
            "t2" => schema! { c2 Int, c3 Varchar, c4 BigInt }
            "t3" => schema! { c1 Int, c2 Varchar, c4 BigInt }
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
        select_with_projection,
        {
            "t1" => schema! { c1 Int, c2 Varchar, c3 BigInt,
                     c4 BigInt, c5 BigInt}
        },
        "SELECT c1, c2, c3, c4 AS column_four FROM t1 WHERE c5 = '' AND column_four > 10",
        "\
Projection [c1, c2, c3, c4 AS column_four]
    Filter [c5 = '' AND column_four > 10]
        Scan table=t1 alias= oid=0
"
    );

    test_statement!(
        select_with_derived_tables,
        {
            "t1" => schema! { c1 Int, c2 Varchar, c3 BigInt }
            "t2" => schema! { c2 Int, c3 Varchar, c4 BigInt }
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
        select_from_values,
        "SELECT * FROM VALUES (1, 2, 3), (4, 5, 6)",
        "\
Projection [*]
    Values [(1, 2, 3), (4, 5, 6)]
"
    );

    test_statement!(
        insert_from_values,
        {
            "t1" => schema! { c1 Int, c2 Int, c3 Int }
        },
        "INSERT INTO t1 VALUES (1, 2, 3), (4, 5, 6)",
        "\
Insert table=t1 oid=0
    Values [(1, 2, 3), (4, 5, 6)]
"
    );

    test_statement!(
        insert_from_derived_table,
        {
            "t1" => schema! { c1 Int, c2 Int, c3 Int }
        },
        "INSERT INTO t1 (SELECT * FROM VALUES (1, 2, 3), (4, 5, 6))",
        "\
Insert table=t1 oid=0
    Projection [*]
        Values [(1, 2, 3), (4, 5, 6)]
"
    );

    test_statement!(
        create_table,
        "CREATE TABLE t1 (c1 INT, c2 VARCHAR)",
        "\
Create table=t1 schema=[c1 INT, c2 VARCHAR]
"
    );
}
