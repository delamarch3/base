use std::sync::Arc;

use crate::catalog::schema::{Schema, Type};
use crate::catalog::TableInfo;
use crate::sql::{Expr, Function, FunctionName, Ident, Literal, Op, SelectItem};

pub mod expr;
pub mod planner;

mod operators;

pub use operators::ProjectionAttributes;
use operators::{Aggregate, Filter, Group, Insert, Join, Limit, Projection, Scan, Sort, Values};

/// The first value will always be Some(..) unless it's a Scan. Binary operators like joins should
/// have both
pub type LogicalPlanInputs<'a> = (Option<&'a LogicalPlan>, Option<&'a LogicalPlan>);

pub enum LogicalPlanError {
    UnknownTable(String),
    UnknownColumn(String),
    NotImplemented(&'static str),
    SchemaMismatch,
    Internal,
}
use LogicalPlanError::*;

impl std::error::Error for LogicalPlanError {}

impl std::fmt::Display for LogicalPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logical plan error: ")?;
        match self {
            LogicalPlanError::UnknownTable(table) => write!(f, "unknown table: {table}"),
            LogicalPlanError::UnknownColumn(column) => write!(f, "unknown column: {column}"),
            LogicalPlanError::NotImplemented(msg) => write!(f, "not implemented: {msg}"),
            LogicalPlanError::SchemaMismatch => write!(f, "schema mismatch"),
            LogicalPlanError::Internal => write!(f, "internal"),
        }
    }
}

impl std::fmt::Debug for LogicalPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub enum LogicalPlan {
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
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt(
            f: &mut std::fmt::Formatter<'_>,
            plan: &LogicalPlan,
            indent: u16,
        ) -> std::fmt::Result {
            (0..indent * 4).try_for_each(|_| write!(f, " "))?;
            match plan {
                LogicalPlan::Aggregate(aggregate) => writeln!(f, "{aggregate}"),
                LogicalPlan::Filter(filter) => writeln!(f, "{filter}"),
                LogicalPlan::Group(group) => writeln!(f, "{group}"),
                LogicalPlan::Join(join) => writeln!(f, "{join}"),
                LogicalPlan::Projection(projection) => writeln!(f, "{projection}"),
                LogicalPlan::Scan(scan) => writeln!(f, "{scan}"),
                LogicalPlan::Limit(limit) => writeln!(f, "{limit}"),
                LogicalPlan::Sort(sort) => writeln!(f, "{sort}"),
                LogicalPlan::Values(values) => writeln!(f, "{values}"),
                LogicalPlan::Insert(insert) => writeln!(f, "{insert}"),
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

impl LogicalPlan {
    pub fn inputs(&self) -> LogicalPlanInputs<'_> {
        match self {
            LogicalPlan::Aggregate(aggregate) => (Some(aggregate.input.as_ref()), None),
            LogicalPlan::Filter(filter) => (Some(filter.input.as_ref()), None),
            LogicalPlan::Group(group) => (Some(group.input.as_ref()), None),
            LogicalPlan::Join(join) => {
                (Some(join.left_input.as_ref()), Some(join.right_input.as_ref()))
            }
            LogicalPlan::Projection(projection) => (Some(projection.input.as_ref()), None),
            LogicalPlan::Scan(_) => (None, None),
            LogicalPlan::Limit(limit) => (Some(limit.input.as_ref()), None),
            LogicalPlan::Sort(sort) => (Some(sort.input.as_ref()), None),
            LogicalPlan::Values(_) => (None, None),
            LogicalPlan::Insert(insert) => (Some(insert.input.as_ref()), None),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Aggregate(aggregate) => aggregate.input.schema(),
            LogicalPlan::Filter(filter) => filter.input.schema(),
            LogicalPlan::Group(group) => group.input.schema(),
            LogicalPlan::Join(join) => &join.schema,
            LogicalPlan::Projection(projection) => &projection.attributes.schema(),
            LogicalPlan::Scan(scan) => &scan.schema,
            LogicalPlan::Limit(limit) => limit.input.schema(),
            LogicalPlan::Sort(sort) => sort.input.schema(),
            LogicalPlan::Values(values) => values.schema(),
            LogicalPlan::Insert(insert) => insert.schema(),
        }
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        match self {
            LogicalPlan::Aggregate(aggregate) => aggregate.input.schema_mut(),
            LogicalPlan::Filter(filter) => filter.input.schema_mut(),
            LogicalPlan::Group(group) => group.input.schema_mut(),
            LogicalPlan::Join(join) => &mut join.schema,
            LogicalPlan::Projection(projection) => projection.attributes.schema_mut(),
            LogicalPlan::Scan(scan) => &mut scan.schema,
            LogicalPlan::Limit(limit) => limit.input.schema_mut(),
            LogicalPlan::Sort(sort) => sort.input.schema_mut(),
            LogicalPlan::Values(values) => values.schema_mut(),
            LogicalPlan::Insert(insert) => insert.schema_mut(),
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

fn expr_type(expr: &Expr, schema: &Schema) -> Result<Type, LogicalPlanError> {
    let ty = match expr {
        Expr::Ident(ident @ Ident::Single(column)) => {
            schema.find_column_by_name(column).ok_or(UnknownColumn(ident.to_string()))?.ty
        }
        Expr::Ident(ident @ Ident::Compound(idents)) => {
            schema
                .find_column_by_name_and_table(&idents[0], &idents[1])
                .ok_or(UnknownColumn(ident.to_string()))?
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
    root: LogicalPlan,
}

pub fn scan(table_info: Arc<TableInfo>) -> Builder {
    Builder { root: LogicalPlan::Scan(Scan::new(table_info)) }
}

pub fn scan_with_alias(table_info: Arc<TableInfo>, alias: String) -> Builder {
    Builder { root: LogicalPlan::Scan(Scan::new_with_alias(table_info, alias)) }
}

pub fn values(values: Vec<Vec<Expr>>) -> Result<Builder, LogicalPlanError> {
    Ok(Builder { root: LogicalPlan::Values(Values::new(values)?) })
}

pub fn values_with_alias(
    values: Vec<Vec<Expr>>,
    alias: String,
) -> Result<Builder, LogicalPlanError> {
    Ok(Builder { root: LogicalPlan::Values(Values::new_with_alias(values, alias)?) })
}

impl Builder {
    pub fn schema(&self) -> &Schema {
        self.root.schema()
    }

    pub fn schema_mut(&mut self) -> &mut Schema {
        self.root.schema_mut()
    }

    pub fn project(self, projection: Vec<SelectItem>) -> Result<Self, LogicalPlanError> {
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
        rhs: impl Into<LogicalPlan>,
        expr: Expr,
    ) -> Result<Self, LogicalPlanError> {
        let lhs = self.root;
        let join = Join::on(expr, lhs, rhs)?;

        Ok(Self { root: join.into() })
    }

    pub fn join_using(
        self,
        rhs: impl Into<LogicalPlan>,
        columns: Vec<Ident>,
    ) -> Result<Self, LogicalPlanError> {
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

    pub fn limit(self, expr: Expr) -> Self {
        let input = self.root;
        let limit = Limit::new(expr, input);

        Self { root: limit.into() }
    }

    pub fn insert(self, table_info: Arc<TableInfo>) -> Result<Self, LogicalPlanError> {
        let input = self.root;
        let insert = Insert::new(table_info, input)?;

        Ok(Self { root: insert.into() })
    }

    pub fn build(self) -> LogicalPlan {
        self.root
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::catalog::Catalog;
    use crate::disk::Memory;
    use crate::logical_plan::{
        expr::{alias, concat, ident, lit, wildcard},
        scan,
    };
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::{column, schema};

    #[test]
    fn test_builder() -> Result<(), LogicalPlanError> {
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
            .limit(lit(5))
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
