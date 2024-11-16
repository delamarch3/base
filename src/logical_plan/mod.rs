use self::expr::{Expr, Function};
use crate::{
    catalog::{IndexInfo, Schema, TableInfo},
    disk::Disk,
};

pub mod expr;

mod aggregate;
mod filter;
mod group;
mod index_scan;
mod join;
mod projection;
mod scan;

pub use {
    aggregate::Aggregate,
    filter::Filter,
    group::Group,
    index_scan::IndexScan,
    join::{Join, JoinAlgorithm},
    projection::Projection,
    scan::Scan,
};

/// The first value will always be Some(..) unless it's a Scan. Binary operators like joins should
/// have both
pub type LogicalPlanInputs<'a> = (Option<&'a LogicalPlan>, Option<&'a LogicalPlan>);

pub enum LogicalPlan {
    Aggregate(Aggregate),
    Filter(Filter),
    Group(Group),
    IndexScan(IndexScan),
    Join(Join),
    Projection(Projection),
    Scan(Scan),
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
    pub fn project(self, exprs: &[&str]) -> Self {
        let input = self.root;
        let projection = Projection::new(exprs, input);

        Self { root: LogicalPlan::Projection(projection) }
    }

    pub fn filter(self, expr: Expr) -> Self {
        let input = self.root;
        let filter = Filter::new(expr, input);

        Self { root: LogicalPlan::Filter(filter) }
    }

    pub fn join(self, rhs: impl Into<LogicalPlan>, predicate: Expr) -> Self {
        let lhs = self.root;
        let join = Join::new(JoinAlgorithm::NestedLoop, predicate, lhs, rhs);

        Self { root: LogicalPlan::Join(join) }
    }

    pub fn group(self, keys: Vec<Expr>) -> Self {
        let input = self.root;
        let group = Group::new(keys, input);

        Self { root: LogicalPlan::Group(group) }
    }

    pub fn aggregate(self, function: Function, keys: Vec<Expr>) -> Self {
        let input = self.root;
        let aggregate = Aggregate::new(function, keys, input);

        Self { root: LogicalPlan::Aggregate(aggregate) }
    }

    pub fn build(self) -> LogicalPlan {
        self.root
    }
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::{
            catalog::{Catalog, Type},
            disk::Memory,
            logical_plan::{
                expr::{ident, lit},
                scan,
            },
            page::PAGE_SIZE,
            page_cache::PageCache,
            replacer::LRU,
        },
    };

    #[test]
    fn test_builder() {
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

        let plan = scan(&t1)
            .filter(ident("c1").is_not_null())
            .join(
                scan(&t2).filter(lit(1).eq(lit(1).and(lit("1").eq(lit("1"))))).build(),
                ident("t1.c3").eq(ident("t2.c3")),
            )
            .project(&["c1"])
            .build();

        let have = plan.to_string();
        let want = "\
Projection [c1]
    NestedLoopJoin [t1.c3 = t2.c3]
        Filter [c1 IS NOT NULL]
            Scan t1 0
        Filter [1 = 1 AND \"1\" = \"1\"]
            Scan t2 1
";

        assert_eq!(want, have)
    }

    #[test]
    fn test_format_logical_plan() {
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
        let projection = Projection::new(&["c1", "c2"], join);

        let plan = LogicalPlan::Projection(projection);

        let have = plan.to_string();
        let want = "\
Projection [c1,c2]
    NestedLoopJoin [t1.c3 = t2.c3]
        Filter [c1 IS NULL AND 5 < c2]
            Scan t1 0
        Filter [c5 IS NOT NULL]
            Scan t2 1
";

        assert_eq!(want, have)
    }
}
