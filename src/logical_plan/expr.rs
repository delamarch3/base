use crate::catalog::Type;

// TODO: all of this will come from the parser and be used as is
use super::write_iter;

pub enum FunctionName {
    Min,
    Max,
    Sum,
    Avg,
    Count,
    Contains,
    Concat,
}

impl std::fmt::Display for FunctionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionName::Min => write!(f, "MIN"),
            FunctionName::Max => write!(f, "MAX"),
            FunctionName::Sum => write!(f, "SUM"),
            FunctionName::Avg => write!(f, "AVG"),
            FunctionName::Count => write!(f, "COUNT"),
            FunctionName::Contains => write!(f, "CONTAINS"),
            FunctionName::Concat => write!(f, "CONCAT"),
        }
    }
}

pub struct Function {
    pub name: FunctionName,
    pub args: Vec<Expr>,
    pub distinct: bool,
}

impl std::fmt::Display for Function {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Function { name, args, distinct } = self;
        write!(f, "{name}(")?;
        if *distinct {
            write!(f, "DISTINCT ")?;
        }
        write_iter(f, &mut args.iter(), ",")?;
        write!(f, ")")
    }
}

pub enum Value {
    Number(String),
    String(String),
    Bool(bool),
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Number(number) => write!(f, "{number}"),
            Value::String(string) => write!(f, "\"{string}\""),
            Value::Bool(bool) => write!(f, "{}", if *bool { "TRUE" } else { "FALSE" }),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl From<i32> for Value {
    fn from(int: i32) -> Self {
        Self::Number(int.to_string())
    }
}

impl From<f32> for Value {
    fn from(float: f32) -> Self {
        Self::Number(float.to_string())
    }
}

impl<'a> From<&'a str> for Value {
    fn from(string: &'a str) -> Self {
        Self::String(string.to_owned())
    }
}

impl From<bool> for Value {
    fn from(bool: bool) -> Self {
        Self::Bool(bool)
    }
}

impl From<&Value> for Type {
    fn from(value: &Value) -> Self {
        match value {
            Value::Number(_) => Type::Int,
            Value::String(_) => Type::Varchar,
            Value::Bool(_) => Type::Bool,
            Value::Null => todo!(),
        }
    }
}

pub enum Expr {
    Ident(String),
    Value(Value), // TODO: keep the parser values, translate to schema values later
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr> },
    BinaryOp { left: Box<Expr>, op: Op, right: Box<Expr> },
    Function(Function),
}

#[derive(PartialEq, Debug, Copy, Clone)]
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
            Expr::Function(function) => write!(f, "{function}"),
        }
    }
}

pub fn ident(ident: &str) -> Expr {
    Expr::Ident(ident.into())
}

pub fn lit(value: impl Into<Value>) -> Expr {
    Expr::Value(value.into())
}

pub fn null() -> Expr {
    Expr::Value(Value::Null)
}

pub fn distinct(expr: Expr) -> Expr {
    match expr {
        Expr::Function(mut function) => {
            function.distinct = true;
            Expr::Function(function)
        }
        _ => expr,
    }
}

pub fn min(expr: Expr) -> Expr {
    Expr::Function(Function { name: FunctionName::Min, args: vec![expr], distinct: false })
}

pub fn max(expr: Expr) -> Expr {
    Expr::Function(Function { name: FunctionName::Max, args: vec![expr], distinct: false })
}

pub fn sum(expr: Expr) -> Expr {
    Expr::Function(Function { name: FunctionName::Sum, args: vec![expr], distinct: false })
}

pub fn avg(expr: Expr) -> Expr {
    Expr::Function(Function { name: FunctionName::Avg, args: vec![expr], distinct: false })
}

pub fn count(expr: Expr) -> Expr {
    Expr::Function(Function { name: FunctionName::Count, args: vec![expr], distinct: false })
}

pub fn contains(args: Vec<Expr>) -> Expr {
    Expr::Function(Function { name: FunctionName::Contains, args, distinct: false })
}

pub fn concat(args: Vec<Expr>) -> Expr {
    Expr::Function(Function { name: FunctionName::Concat, args, distinct: false })
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
