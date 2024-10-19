use crate::{catalog::Schema, table::tuple::Value};

mod filter;
mod join;
mod projection;
mod scan;

pub use {
    filter::Filter,
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
    Value(Value),
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

#[cfg(test)]
mod test {
    use {
        super::{format_logical_plan, Expr, Filter, Join, JoinAlgorithm, Op, Projection, Scan},
        crate::{catalog::Type, table::tuple::Value},
    };

    #[test]
    fn test_format_logical_plan() {
        let schema_a = [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)].into();
        let scan_a = Scan::new("t1".into(), schema_a);
        let filter_expr_a = Expr::BinaryOp {
            left: Box::new(Expr::IsNull(Box::new(Expr::Ident("c1".into())))),
            op: Op::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Int(5))),
                op: Op::Lt,
                right: Box::new(Expr::Ident("c2".into())),
            }),
        };
        let filter_a = Filter::new(filter_expr_a, Box::new(scan_a));

        let schema_b = [("c3", Type::Int), ("c4", Type::Varchar), ("c5", Type::BigInt)].into();
        let scan_b = Scan::new("t2".into(), schema_b);
        let filter_expr_b = Expr::IsNotNull(Box::new(Expr::Ident("c5".into())));
        let filter_b = Filter::new(filter_expr_b, Box::new(scan_b));

        let join = Join::new(
            JoinAlgorithm::BlockNestedLoopJoin,
            [Expr::Ident("t1".into()), Expr::Ident("t2".into())],
            Expr::BinaryOp {
                left: Box::new(Expr::Ident("t1.c3".into())),
                op: Op::Eq,
                right: Box::new(Expr::Ident("t2.c3".into())),
            },
            Box::new(filter_a),
            Box::new(filter_b),
        );
        let projection = Projection::new(&["c1", "c2"], Box::new(join));

        let have = format_logical_plan(&projection);
        let want = "\
Projection: c1,c2
	BlockNestedLoopJoin: tables=t1,t2 expr=t1.c3 = t2.c3
		Filter: expr=c1 IS NULL AND 5 < c2
			Scan: table=t1
		Filter: expr=c5 IS NOT NULL
			Scan: table=t2
";

        assert_eq!(want, have)
    }
}
