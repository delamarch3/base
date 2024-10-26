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
pub type LogicalPlanInputs<'a> =
    (Option<&'a Box<dyn LogicalPlan>>, Option<&'a Box<dyn LogicalPlan>>);

pub trait LogicalPlan: std::fmt::Display {
    fn schema(&self) -> &Schema;
    fn inputs(&self) -> LogicalPlanInputs;
}

pub fn format_logical_plan(plan: &dyn LogicalPlan) -> String {
    fn format_logical_plan(plan: &dyn LogicalPlan, indent: u16) -> String {
        let mut output = String::new();
        (0..indent).for_each(|_| output.push('\t'));
        output.push_str(&plan.to_string());
        output.push('\n');

        let (lhs, rhs) = plan.inputs();
        if let Some(plan) = lhs {
            output.push_str(&format_logical_plan(plan.as_ref(), indent + 1));
        }
        if let Some(plan) = rhs {
            output.push_str(&format_logical_plan(plan.as_ref(), indent + 1));
        }

        output
    }

    format_logical_plan(plan, 0)
}

fn write_iter<T: std::fmt::Display, I: Iterator<Item = T>>(
    f: &mut std::fmt::Formatter<'_>,
    iter: &mut I,
    seperator: &'static str,
) -> std::fmt::Result {
    let mut tmp = "";
    while let Some(item) = iter.next() {
        write!(f, "{tmp}")?;
        tmp = seperator;
        write!(f, "{item}")?;
    }

    Ok(())
}
pub struct Builder {
    root: Box<dyn LogicalPlan>,
}

impl Builder {
    pub fn scan<D: Disk + 'static>(table_info: TableInfo<D>) -> Self {
        Self { root: Box::new(Scan::new(table_info)) }
    }

    pub fn index_scan(index_info: IndexInfo) -> Self {
        Self { root: Box::new(IndexScan::new(index_info)) }
    }

    pub fn project(self, exprs: &[&str]) -> Self {
        let input = self.root;
        let projection = Projection::new(exprs, input);

        Self { root: Box::new(projection) }
    }

    pub fn filter(self, expr: Expr) -> Self {
        let input = self.root;
        let filter = Filter::new(expr, input);

        Self { root: Box::new(filter) }
    }

    pub fn join(self, rhs: Builder, predicate: Expr) -> Self {
        let lhs = self.root;
        let join = Join::new(JoinAlgorithm::NestedLoopJoin, predicate, lhs, rhs.root);

        Self { root: Box::new(join) }
    }

    pub fn group(self, keys: Vec<Expr>) -> Self {
        let input = self.root;
        let group = Group::new(keys, input);

        Self { root: Box::new(group) }
    }

    pub fn aggregate(self, function: Function, keys: Vec<Expr>) -> Self {
        let input = self.root;
        let aggregate = Aggregate::new(function, keys, input);

        Self { root: Box::new(aggregate) }
    }

    pub fn build(self) -> Box<dyn LogicalPlan> {
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
                expr::{ident, number, string},
                format_logical_plan,
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

        let plan = Builder::scan(t1)
            .filter(ident("c1").is_not_null())
            .join(
                Builder::scan(t2)
                    .filter(number("1").eq(number("1").and(string("1").eq(string("1"))))),
                ident("t1.c3").eq(ident("t2.c3")),
            )
            .project(&["c1"])
            .build();

        let have = format_logical_plan(&*plan);
        let want = "\
Projection [c1]
	BlockNestedLoopJoin [t1.c3 = t2.c3]
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

        let scan_a = Scan::new(t1);
        let filter_expr_a = ident("c1").is_null().and(number("5").lt(ident("c2")));
        let filter_a = Filter::new(filter_expr_a, Box::new(scan_a));

        let scan_b = Scan::new(t2);
        let filter_expr_b = ident("c5").is_not_null();
        let filter_b = Filter::new(filter_expr_b, Box::new(scan_b));

        let join = Join::new(
            JoinAlgorithm::NestedLoopJoin,
            ident("t1.c3").eq(ident("t2.c3")),
            Box::new(filter_a),
            Box::new(filter_b),
        );
        let projection = Projection::new(&["c1", "c2"], Box::new(join));

        let have = format_logical_plan(&projection);
        let want = "\
Projection [c1,c2]
	BlockNestedLoopJoin [t1.c3 = t2.c3]
		Filter [c1 IS NULL AND 5 < c2]
			Scan t1 0
		Filter [c5 IS NOT NULL]
			Scan t2 1
";

        assert_eq!(want, have)
    }
}
