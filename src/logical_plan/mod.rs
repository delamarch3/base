use std::sync::Arc;

use crate::catalog::schema::{Schema, Type};
use crate::catalog::TableInfo;
use crate::sql::{Expr, Function, FunctionName, Ident, Literal, Op, SelectItem};

mod aggregate;
mod create;
mod filter;
mod group;
mod insert;
mod join;
mod limit;
mod projection;
mod scan;
mod sort;
mod values;

pub use projection::ProjectionAttributes;
use {
    aggregate::Aggregate, create::Create, filter::Filter, group::Group, insert::Insert, join::Join,
    limit::Limit, projection::Projection, scan::Scan, sort::Sort, values::Values,
};

/// The first value will always be Some(..) unless it's a leaf node like Scan.
/// Binary operators like joins should have both
pub type LogicalOperatorInputs<'a> = (Option<&'a LogicalOperator>, Option<&'a LogicalOperator>);

pub struct LogicalOperatorError(String);
impl std::error::Error for LogicalOperatorError {}

impl std::fmt::Display for LogicalOperatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logical operator error: {}", self.0)
    }
}

impl std::fmt::Debug for LogicalOperatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<String> for LogicalOperatorError {
    fn from(value: String) -> Self {
        LogicalOperatorError(value)
    }
}

impl From<&str> for LogicalOperatorError {
    fn from(value: &str) -> Self {
        LogicalOperatorError(value.into())
    }
}

pub enum LogicalOperator {
    Aggregate(Aggregate),
    Filter(Filter),
    Group(Group),
    Join(Join),
    Projection(Projection),
    Scan(Scan),
    Limit(Limit),
    Sort(Sort),
    Values(Values),
    Insert(Insert),
    Create(Create),
}

impl std::fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt(
            f: &mut std::fmt::Formatter<'_>,
            plan: &LogicalOperator,
            indent: usize,
        ) -> std::fmt::Result {
            let spaces = indent * 4;
            write!(f, "{:spaces$}", "")?;

            match plan {
                LogicalOperator::Aggregate(aggregate) => writeln!(f, "{aggregate}"),
                LogicalOperator::Filter(filter) => writeln!(f, "{filter}"),
                LogicalOperator::Group(group) => writeln!(f, "{group}"),
                LogicalOperator::Join(join) => writeln!(f, "{join}"),
                LogicalOperator::Projection(projection) => {
                    writeln!(f, "{projection}")
                }
                LogicalOperator::Scan(scan) => writeln!(f, "{scan}"),
                LogicalOperator::Limit(limit) => writeln!(f, "{limit}"),
                LogicalOperator::Sort(sort) => writeln!(f, "{sort}"),
                LogicalOperator::Values(values) => writeln!(f, "{values}"),
                LogicalOperator::Insert(insert) => writeln!(f, "{insert}"),
                LogicalOperator::Create(create) => writeln!(f, "{create}"),
            }?;

            let (lhs, rhs) = plan.inputs();
            if let Some(plan) = lhs {
                fmt(f, plan, indent + 1)?;
            }
            if let Some(plan) = rhs {
                fmt(f, plan, indent + 1)?;
            }

            Ok(())
        }

        fmt(f, self, 0)
    }
}

impl LogicalOperator {
    pub fn inputs(&self) -> LogicalOperatorInputs<'_> {
        match self {
            LogicalOperator::Aggregate(aggregate) => (Some(aggregate.input.as_ref()), None),
            LogicalOperator::Filter(filter) => (Some(filter.input.as_ref()), None),
            LogicalOperator::Group(group) => (Some(group.input.as_ref()), None),
            LogicalOperator::Join(join) => {
                (Some(join.left_input.as_ref()), Some(join.right_input.as_ref()))
            }
            LogicalOperator::Projection(projection) => (Some(projection.input.as_ref()), None),
            LogicalOperator::Scan(_) => (None, None),
            LogicalOperator::Limit(limit) => (Some(limit.input.as_ref()), None),
            LogicalOperator::Sort(sort) => (Some(sort.input.as_ref()), None),
            LogicalOperator::Values(_) => (None, None),
            LogicalOperator::Insert(insert) => (Some(insert.input.as_ref()), None),
            LogicalOperator::Create(_) => (None, None),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            LogicalOperator::Aggregate(aggregate) => aggregate.input.schema(),
            LogicalOperator::Filter(filter) => filter.input.schema(),
            LogicalOperator::Group(group) => group.input.schema(),
            LogicalOperator::Join(join) => &join.schema,
            LogicalOperator::Projection(projection) => projection.attributes.schema(),
            LogicalOperator::Scan(scan) => &scan.schema,
            LogicalOperator::Limit(limit) => limit.input.schema(),
            LogicalOperator::Sort(sort) => sort.input.schema(),
            LogicalOperator::Values(values) => values.schema(),
            LogicalOperator::Insert(insert) => insert.schema(),
            LogicalOperator::Create(create) => create.schema(),
        }
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        match self {
            LogicalOperator::Aggregate(aggregate) => aggregate.input.schema_mut(),
            LogicalOperator::Filter(filter) => filter.input.schema_mut(),
            LogicalOperator::Group(group) => group.input.schema_mut(),
            LogicalOperator::Join(join) => &mut join.schema,
            LogicalOperator::Projection(projection) => projection.attributes.schema_mut(),
            LogicalOperator::Scan(scan) => &mut scan.schema,
            LogicalOperator::Limit(limit) => limit.input.schema_mut(),
            LogicalOperator::Sort(sort) => sort.input.schema_mut(),
            LogicalOperator::Values(values) => values.schema_mut(),
            LogicalOperator::Insert(insert) => insert.schema_mut(),
            LogicalOperator::Create(create) => create.schema_mut(),
        }
    }
}

fn write_iter<T: std::fmt::Display, I: Iterator<Item = T>>(
    f: &mut std::fmt::Formatter<'_>,
    iter: &mut I,
    seperator: &'static str,
) -> std::fmt::Result {
    let mut tmp = "";
    for item in iter.by_ref() {
        write!(f, "{tmp}")?;
        tmp = seperator;
        write!(f, "{item}")?;
    }

    Ok(())
}

fn expr_type(expr: &Expr, schema: &Schema) -> Result<Type, LogicalOperatorError> {
    let ty = match expr {
        Expr::Ident(ident @ Ident::Single(column)) => {
            schema.find_column_by_name(column).ok_or(format!("unknown column: {ident}"))?.ty
        }
        Expr::Ident(ident @ Ident::Compound(idents)) => {
            schema
                .find_column_by_name_and_table(&idents[0], &idents[1])
                .ok_or(format!("unknown column: {ident}"))?
                .ty
        }
        Expr::Literal(literal) => match literal {
            Literal::Number(_) => Type::Int,
            Literal::String(_) => Type::Varchar,
            Literal::Bool(_) => Type::Bool,
            Literal::Null | Literal::Decimal(_) => todo!(),
        },
        Expr::IsNull { .. } | Expr::InList { .. } | Expr::Between { .. } => Type::Bool,
        Expr::BinaryOp { left: _, op, right: _ } => match op {
            Op::Eq | Op::Neq | Op::Lt | Op::Le | Op::Gt | Op::Ge | Op::And | Op::Or => Type::Bool,
        },
        Expr::Function(function) => match function.name {
            FunctionName::Min => Type::Int,
            FunctionName::Max => Type::Int,
            FunctionName::Sum => Type::Int,
            FunctionName::Avg => Type::Int,
            FunctionName::Count => Type::Int,
            FunctionName::Contains => Type::Bool,
            FunctionName::Concat => Type::Varchar,
        },

        Expr::SubQuery(_) => todo!(),
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard(_) => todo!(),
    };

    Ok(ty)
}

pub struct Builder {
    root: LogicalOperator,
}

pub fn create(name: String, schema: Schema) -> Builder {
    Builder { root: LogicalOperator::Create(Create::new(name, schema)) }
}

pub fn scan(table_info: Arc<TableInfo>) -> Builder {
    Builder { root: LogicalOperator::Scan(Scan::new(table_info)) }
}

pub fn scan_with_alias(table_info: Arc<TableInfo>, alias: String) -> Builder {
    Builder { root: LogicalOperator::Scan(Scan::new_with_alias(table_info, alias)) }
}

pub fn values(values: Vec<Vec<Expr>>) -> Result<Builder, LogicalOperatorError> {
    Ok(Builder { root: LogicalOperator::Values(Values::new(values)?) })
}

pub fn values_with_alias(
    values: Vec<Vec<Expr>>,
    alias: String,
) -> Result<Builder, LogicalOperatorError> {
    Ok(Builder { root: LogicalOperator::Values(Values::new_with_alias(values, alias)?) })
}

impl Builder {
    pub fn schema(&self) -> &Schema {
        self.root.schema()
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        self.root.schema_mut()
    }

    pub fn project(self, projection: Vec<SelectItem>) -> Result<Self, LogicalOperatorError> {
        let input = self.root;
        let projection = Projection::new(projection, input)?;

        Ok(Self { root: projection.into() })
    }

    pub fn filter(self, expr: Expr) -> Self {
        let input = self.root;
        let filter = Filter::new(expr, input);

        Self { root: filter.into() }
    }

    pub fn join_on(
        self,
        rhs: impl Into<LogicalOperator>,
        expr: Expr,
    ) -> Result<Self, LogicalOperatorError> {
        let lhs = self.root;
        let join = Join::on(expr, lhs, rhs)?;

        Ok(Self { root: join.into() })
    }

    pub fn join_using(
        self,
        rhs: impl Into<LogicalOperator>,
        columns: Vec<Ident>,
    ) -> Result<Self, LogicalOperatorError> {
        let lhs = self.root;
        let join = Join::using(columns, lhs, rhs)?;

        Ok(Self { root: join.into() })
    }

    pub fn group(self, keys: Vec<Expr>) -> Self {
        let input = self.root;
        let group = Group::new(keys, input);

        Self { root: group.into() }
    }

    pub fn aggregate(self, function: Function, keys: Vec<Expr>) -> Self {
        let input = self.root;
        let aggregate = Aggregate::new(function, keys, input);

        Self { root: aggregate.into() }
    }

    pub fn sort(self, exprs: Vec<Expr>) -> Self {
        let input = self.root;
        let sort = Sort::new(exprs, false, input);

        Self { root: sort.into() }
    }

    pub fn sort_desc(self, exprs: Vec<Expr>) -> Self {
        let input = self.root;
        let sort = Sort::new(exprs, true, input);

        Self { root: sort.into() }
    }

    pub fn limit(self, expr: Expr) -> Result<Self, LogicalOperatorError> {
        let input = self.root;
        let limit = Limit::new(expr, input)?;

        Ok(Self { root: limit.into() })
    }

    pub fn insert(self, table_info: Arc<TableInfo>) -> Result<Self, LogicalOperatorError> {
        let input = self.root;
        let insert = Insert::new(table_info, input)?;

        Ok(Self { root: insert.into() })
    }

    pub fn build(self) -> LogicalOperator {
        self.root
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::catalog::Catalog;
    use crate::disk::Memory;
    use crate::logical_plan::scan;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::sql::expr_builder::{alias, concat, ident, lit, wildcard};
    use crate::{column, schema};

    #[test]
    fn test_builder() -> Result<(), LogicalOperatorError> {
        const MEMORY: usize = PAGE_SIZE * 8;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pc = PageCache::new(disk, replacer, 0);

        let mut catalog = Catalog::new(pc);
        let t1 = catalog
            .create_table(
                "t1",
                schema! {column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt)},
            )
            .unwrap()
            .unwrap()
            .clone();
        let t2 = catalog
            .create_table(
                "t2",
                schema! {column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt)},
            )
            .unwrap()
            .unwrap()
            .clone();

        let plan = scan(t1)
            .filter(ident("c1").is_not_null())
            .join_on(
                scan(t2).filter(lit(1).eq(lit(1).and(lit("1").eq(lit("1"))))).build(),
                ident("t1.c3").eq(ident("t2.c3")),
            )?
            .project(vec![
                ident("c1").into(),
                concat(vec![lit(1), lit("2")]).into(),
                ident("c5").is_null().into(),
                alias(lit(1), "one"),
                wildcard(),
            ])?
            .sort(vec![ident("c1")])
            .limit(lit(5))?
            .build();

        let have = plan.to_string();
        let want = "\
Limit 5
    Sort [c1] ASC
        Projection [c1, CONCAT(1, '2'), c5 IS NULL, 1 AS one, *]
            Join ON t1.c3 = t2.c3
                Filter [c1 IS NOT NULL]
                    Scan table=t1 alias= oid=0
                Filter [1 = 1 AND '1' = '1']
                    Scan table=t2 alias= oid=1
";

        assert_eq!(want, have);

        Ok(())
    }
}
