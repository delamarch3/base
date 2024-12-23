use crate::catalog::{IndexInfo, Schema, TableInfo};
use crate::disk::Disk;
use crate::sql::{Expr, Function, Ident, SelectItem};

pub mod expr;
pub mod planner;

mod aggregate;
mod filter;
mod group;
mod index_scan;
mod join;
mod limit;
mod projection;
mod scan;
mod sort;

use {
    aggregate::Aggregate,
    filter::Filter,
    group::Group,
    index_scan::IndexScan,
    join::{Join, JoinAlgorithm},
    limit::Limit,
    projection::Projection,
    scan::Scan,
    sort::Sort,
};

/// The first value will always be Some(..) unless it's a Scan. Binary operators like joins should
/// have both
pub type LogicalPlanInputs<'a> = (Option<&'a LogicalPlan>, Option<&'a LogicalPlan>);

pub enum LogicalPlanError {
    InvalidIdent(Ident),
}

impl std::error::Error for LogicalPlanError {}

impl std::fmt::Display for LogicalPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logical plan error: ")?;
        match self {
            LogicalPlanError::InvalidIdent(ident) => write!(f, "invalid identifier: {}", ident),
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
    IndexScan(IndexScan),
    Join(Join),
    Projection(Projection),
    Scan(Scan),
    Limit(Limit),
    Sort(Sort),
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
                LogicalPlan::IndexScan(index_scan) => writeln!(f, "{index_scan}"),
                LogicalPlan::Join(join) => writeln!(f, "{join}"),
                LogicalPlan::Projection(projection) => writeln!(f, "{projection}"),
                LogicalPlan::Scan(scan) => writeln!(f, "{scan}"),
                LogicalPlan::Limit(limit) => writeln!(f, "{limit}"),
                LogicalPlan::Sort(sort) => writeln!(f, "{sort}"),
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
            LogicalPlan::IndexScan(_) => (None, None),
            LogicalPlan::Join(join) => {
                (Some(join.left_input.as_ref()), Some(join.right_input.as_ref()))
            }
            LogicalPlan::Projection(projection) => (Some(projection.input.as_ref()), None),
            LogicalPlan::Scan(_) => (None, None),
            LogicalPlan::Limit(limit) => (Some(limit.input.as_ref()), None),
            LogicalPlan::Sort(sort) => (Some(sort.input.as_ref()), None),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Aggregate(aggregate) => aggregate.input.schema(),
            LogicalPlan::Filter(filter) => filter.input.schema(),
            LogicalPlan::Group(group) => group.input.schema(),
            LogicalPlan::IndexScan(index_scan) => &index_scan.index.schema,
            LogicalPlan::Join(join) => &join.schema,
            LogicalPlan::Projection(projection) => &projection.schema,
            LogicalPlan::Scan(scan) => &scan.schema,
            LogicalPlan::Limit(limit) => &limit.input.schema(),
            LogicalPlan::Sort(sort) => &sort.input.schema(),
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

pub struct Builder {
    root: LogicalPlan,
}

pub fn scan<D: Disk>(table_info: &TableInfo<D>) -> Builder {
    Builder { root: LogicalPlan::Scan(Scan::new(table_info)) }
}

pub fn index_scan(index_info: IndexInfo) -> Builder {
    Builder { root: LogicalPlan::IndexScan(IndexScan::new(index_info)) }
}

impl Builder {
    pub fn schema(&self) -> &Schema {
        self.root.schema()
    }

    pub fn project(self, projection: Vec<SelectItem>) -> Self {
        let input = self.root;
        let projection = Projection::new(projection, input);

        Self { root: projection.into() }
    }

    pub fn filter(self, expr: Expr) -> Self {
        let input = self.root;
        let filter = Filter::new(expr, input);

        Self { root: filter.into() }
    }

    pub fn join(self, rhs: impl Into<LogicalPlan>, predicate: Expr) -> Self {
        let lhs = self.root;
        let join = Join::new(JoinAlgorithm::NestedLoop, predicate, lhs, rhs);

        Self { root: join.into() }
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

    pub fn build(self) -> LogicalPlan {
        self.root
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::catalog::{Catalog, Type};
    use crate::disk::Memory;
    use crate::logical_plan::{
        expr::{alias, concat, ident, lit, wildcard},
        scan,
    };
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;

    #[test]
    fn test_builder() -> Result<(), LogicalPlanError> {
        const MEMORY: usize = PAGE_SIZE * 8;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pc = PageCache::new(disk, replacer, 0);

        let mut catalog = Catalog::new(pc);
        let t1 = catalog
            .create_table("t1", [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)])
            .unwrap()
            .unwrap()
            .clone();
        let t2 = catalog
            .create_table("t2", [("c3", Type::Int), ("c4", Type::Varchar), ("c5", Type::BigInt)])
            .unwrap()
            .unwrap()
            .clone();

        let plan = scan(&t1)
            .filter(ident("c1").is_not_null())
            .join(
                scan(&t2).filter(lit(1).eq(lit(1).and(lit("1").eq(lit("1"))))).build(),
                ident("t1.c3").eq(ident("t2.c3")),
            )
            .project(vec![
                ident("c1").into(),
                concat(vec![lit(1), lit("2")]).into(),
                ident("c5").is_null().into(),
                alias(lit(1), "one"),
                wildcard(),
            ])
            .sort(vec![ident("c1")])
            .limit(lit(5))
            .build();

        let have = plan.to_string();
        let want = "\
Limit 5
    Sort [c1] ASC
        Projection [c1, CONCAT(1,\"2\"), c5 IS NULL, 1 AS one, *]
            NestedLoopJoin [t1.c3 = t2.c3]
                Filter [c1 IS NOT NULL]
                    Scan t1 0
                Filter [1 = 1 AND \"1\" = \"1\"]
                    Scan t2 1
";

        assert_eq!(want, have);

        Ok(())
    }

    #[test]
    fn test_format_logical_plan() -> Result<(), LogicalPlanError> {
        const MEMORY: usize = PAGE_SIZE * 8;
        const K: usize = 2;
        let disk = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pc = PageCache::new(disk, replacer, 0);

        let mut catalog = Catalog::new(pc);
        let t1 = catalog
            .create_table("t1", [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)])
            .expect("could not create table")
            .expect("there is no table")
            .clone();
        let t2 = catalog
            .create_table("t2", [("c3", Type::Int), ("c4", Type::Varchar), ("c5", Type::BigInt)])
            .expect("could not create table")
            .expect("there is no table")
            .clone();

        let scan_a = Scan::new(&t1);
        let filter_expr_a = ident("c1").is_null().and(lit(5).lt(ident("c2")));
        let filter_a = Filter::new(filter_expr_a, scan_a);

        let scan_b = Scan::new(&t2);
        let filter_expr_b = ident("c5").is_not_null();
        let filter_b = Filter::new(filter_expr_b, scan_b);

        let join = Join::new(
            JoinAlgorithm::NestedLoop,
            ident("t1.c3").eq(ident("t2.c3")),
            filter_a,
            filter_b,
        );
        let projection = Projection::new(vec![ident("c1").into(), ident("c2").into()], join);

        let plan = LogicalPlan::Projection(projection);

        let have = plan.to_string();
        let want = "\
Projection [c1, c2]
    NestedLoopJoin [t1.c3 = t2.c3]
        Filter [c1 IS NULL AND 5 < c2]
            Scan t1 0
        Filter [c5 IS NOT NULL]
            Scan t2 1
";

        assert_eq!(want, have);

        Ok(())
    }
}
