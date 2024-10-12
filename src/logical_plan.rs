use crate::{catalog::Schema, table::tuple::Value};

/// The first value will always be Some(..) unless it's a Scan. Binary operators like joins should
/// have both
pub type LogicalPlanInputs<'a> =
    (Option<&'a Box<dyn LogicalPlan>>, Option<&'a Box<dyn LogicalPlan>>);

pub trait LogicalPlan: std::fmt::Display {
    fn schema(&self) -> &Schema;
    fn inputs(&self) -> LogicalPlanInputs;
}

pub fn format_logical_plan(plan: &Box<dyn LogicalPlan>) -> String {
    fn format_logical_plan(plan: &Box<dyn LogicalPlan>, indent: u16) -> String {
        let mut output = String::new();
        (0..indent).for_each(|_| output.push('\t'));
        output.push_str(&plan.to_string());
        output.push('\n');

        let (lhs, rhs) = plan.inputs();
        if let Some(plan) = lhs {
            output.push_str(&format_logical_plan(&plan, indent + 1));
        }
        if let Some(plan) = rhs {
            output.push_str(&format_logical_plan(&plan, indent + 1));
        }

        output
    }

    format_logical_plan(plan, 0)
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
                for expr in list {
                    write!(f, "{expr},")?;
                }
                write!(f, "]")
            }
            Expr::InList { expr, list, negated: true } => {
                write!(f, "{expr} NOT IN [")?;
                for expr in list {
                    write!(f, "{expr},")?;
                }
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

pub struct Scan {
    table: String,
    schema: Schema,
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;

        write!(f, "Scan: table={}, projection=", self.table)?;
        let mut exprs = String::new();
        for column in self.schema.columns() {
            write!(exprs, "{},", column.name)?;
        }
        exprs.pop();

        write!(f, "{}", exprs)
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (None, None)
    }
}

impl Scan {
    pub fn new(table: String, schema: Schema) -> Self {
        Self { table, schema }
    }
}

pub struct Filter {
    expr: Expr,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: expr={}", self.expr)?;

        Ok(())
    }
}

impl LogicalPlan for Filter {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Filter {
    pub fn new(expr: Expr, input: Box<dyn LogicalPlan>) -> Self {
        Self { expr, input }
    }
}

// TODO: keeping this simple for now but should be able to support expressions too
pub struct Projection {
    schema: Schema,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;

        write!(f, "Projection: ")?;
        let mut exprs = String::new();
        for column in self.schema.columns() {
            write!(exprs, "{},", column.name)?;
        }
        exprs.pop();

        write!(f, "{}", exprs)
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Projection {
    pub fn new(exprs: &[&str], input: Box<dyn LogicalPlan>) -> Self {
        let schema = input.schema().filter(exprs);
        Self { schema, input }
    }
}

#[cfg(test)]
mod test {
    use {
        super::{format_logical_plan, Expr, Filter, LogicalPlan, Op, Projection, Scan},
        crate::{catalog::Type, table::tuple::Value},
    };

    #[test]
    fn test_format_logical_plan() {
        let schema = [("c1", Type::Int), ("c2", Type::Varchar), ("c3", Type::BigInt)].into();
        let scan = Scan::new("t1".into(), schema);
        let filter_expr = Expr::BinaryOp {
            left: Box::new(Expr::IsNull(Box::new(Expr::Ident("c1".into())))),
            op: Op::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Value(Value::Int(5))),
                op: Op::Lt,
                right: Box::new(Expr::Ident("c2".into())),
            }),
        };
        let filter = Filter::new(filter_expr, Box::new(scan));
        let projection = Projection::new(&["c1", "c2"], Box::new(filter));

        let have = format_logical_plan(&(Box::new(projection) as Box<dyn LogicalPlan>));
        let want = "\
Projection: c1,c2
	Filter: expr=c1 IS NULL AND 5 < c2
		Scan: table=t1, projection=c1,c2,c3
";

        assert_eq!(want, have)
    }
}
