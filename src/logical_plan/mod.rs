use crate::{
    catalog::{IndexInfo, Schema, TableInfo},
    disk::Disk,
    table::tuple::Value,
};

mod filter;
mod index_scan;
mod join;
mod projection;
mod scan;

pub use {
    filter::Filter,
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

// TODO: it's probably ok to use Expr from the parser once that's ready
pub enum Expr {
    Ident(String),
    Value(Value), // TODO: keep the parser values, translate to schema values later
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr> },
    BinaryOp { left: Box<Expr>, op: Op, right: Box<Expr> },
}

#[derive(PartialEq, Debug)]
pub enum Op {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Eq => write!(f, "="),
            Op::Neq => write!(f, "!="),
            Op::Lt => write!(f, "<"),
            Op::Le => write!(f, "<="),
            Op::Gt => write!(f, ">"),
            Op::Ge => write!(f, ">="),
            Op::And => write!(f, "AND"),
            Op::Or => write!(f, "OR"),
        }
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Ident(ident) => write!(f, "{ident}"),
            Expr::Value(value) => write!(f, "{value}"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::InList { expr, list, negated: false } => {
                write!(f, "{expr} IN [")?;
                write_iter(f, &mut list.iter(), ",")?;
                write!(f, "]")
            }
            Expr::InList { expr, list, negated: true } => {
                write!(f, "{expr} NOT IN [")?;
                write_iter(f, &mut list.iter(), ",")?;
                write!(f, "]")
            }
            Expr::Between { expr, negated: false, low, high } => {
                write!(f, "{expr} BETWEEN {low} AND {high}")
            }
            Expr::Between { expr, negated: true, low, high } => {
                write!(f, "{expr} NOT BETWEEN {low} AND {high}")
            }
            Expr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
        }
    }
}

pub fn ident(ident: &str) -> Expr {
    Expr::Ident(ident.into())
}

pub fn int(int: i32) -> Expr {
    Expr::Value(Value::Int(int))
}

pub fn string(string: &str) -> Expr {
    Expr::Value(Value::Varchar(string.into()))
}

impl Expr {
    pub fn is_null(self) -> Self {
        Expr::IsNull(Box::new(self))
    }

    pub fn is_not_null(self) -> Self {
        Expr::IsNotNull(Box::new(self))
    }

    pub fn in_list(self, list: Vec<Expr>) -> Self {
        Expr::InList { expr: Box::new(self), list, negated: false }
    }

    pub fn not_in_list(self, list: Vec<Expr>) -> Self {
        Expr::InList { expr: Box::new(self), list, negated: true }
    }

    pub fn between(self, low: Expr, high: Expr) -> Self {
        Expr::Between {
            expr: Box::new(self),
            negated: false,
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    pub fn not_between(self, low: Expr, high: Expr) -> Self {
        Expr::Between {
            expr: Box::new(self),
            negated: true,
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    pub fn eq(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Eq, right: Box::new(rhs) }
    }

    pub fn neq(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Neq, right: Box::new(rhs) }
    }

    pub fn lt(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Lt, right: Box::new(rhs) }
    }

    pub fn le(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Le, right: Box::new(rhs) }
    }

    pub fn gt(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Gt, right: Box::new(rhs) }
    }

    pub fn ge(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Ge, right: Box::new(rhs) }
    }

    pub fn and(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::And, right: Box::new(rhs) }
    }

    pub fn or(self, rhs: Expr) -> Self {
        Expr::BinaryOp { left: Box::new(self), op: Op::Or, right: Box::new(rhs) }
    }
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
            logical_plan::{format_logical_plan, ident, int, string},
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
                Builder::scan(t2).filter(int(1).eq(int(1).and(string("1").eq(string("1"))))),
                ident("t1.c3").eq(ident("t2.c3")),
            )
            .project(&["c1"])
            .build();

        let have = format_logical_plan(&*plan);
        let want = "\
Projection: c1
	BlockNestedLoopJoin: expr=[t1.c3 = t2.c3]
		Filter: expr=[c1 IS NOT NULL]
			Scan: table=t1 oid=0
		Filter: expr=[1 = 1 AND \"1\" = \"1\"]
			Scan: table=t2 oid=1
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
        let filter_expr_a = ident("c1").is_null().and(int(5).lt(ident("c2")));
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
Projection: c1,c2
	BlockNestedLoopJoin: expr=[t1.c3 = t2.c3]
		Filter: expr=[c1 IS NULL AND 5 < c2]
			Scan: table=t1 oid=0
		Filter: expr=[c5 IS NOT NULL]
			Scan: table=t2 oid=1
";

        assert_eq!(want, have)
    }
}
