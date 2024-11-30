use crate::catalog::Type;
use crate::sql::{Expr, Function, FunctionName, Literal, Op, Parser};

impl From<i32> for Literal {
    fn from(int: i32) -> Self {
        Self::Number(int.to_string())
    }
}

impl From<f32> for Literal {
    fn from(float: f32) -> Self {
        Self::Number(float.to_string())
    }
}

impl<'a> From<&'a str> for Literal {
    fn from(string: &'a str) -> Self {
        Self::String(string.to_owned())
    }
}

impl From<bool> for Literal {
    fn from(bool: bool) -> Self {
        Self::Bool(bool)
    }
}

impl From<&Literal> for Type {
    fn from(value: &Literal) -> Self {
        match value {
            Literal::Number(_) => Type::Int,
            Literal::String(_) => Type::Varchar,
            Literal::Bool(_) => Type::Bool,
            Literal::Decimal(_) => todo!(),
            Literal::Null => todo!(),
        }
    }
}

pub fn ident(ident: &str) -> Expr {
    let ident = Parser::new(&ident).expect("invalid input").parse_ident().expect("invalid ident");
    Expr::Ident(ident)
}

pub fn lit(value: impl Into<Literal>) -> Expr {
    Expr::Literal(value.into())
}

pub fn null() -> Expr {
    Expr::Literal(Literal::Null)
}

pub fn distinct(expr: Expr) -> Expr {
    match expr {
        Expr::Function(Function { name, args, distinct: _ }) => {
            Expr::Function(Function { name, args, distinct: true })
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
        Expr::IsNull { expr: Box::new(self), negated: false }
    }

    pub fn is_not_null(self) -> Self {
        Expr::IsNull { expr: Box::new(self), negated: true }
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
