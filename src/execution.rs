use crate::catalog::schema::{Column, Schema, Type};
use crate::get_value;
use crate::sql::{Expr, Function, FunctionName, Ident, Literal, Op};
use crate::table::tuple::{Data as TupleData, Value};

macro_rules! get_value {
    ($value:ident, $type:tt) => {
        match $value {
            Value::$type(value) => value,
            _ => unreachable!(),
        }
    };
}

#[derive(PartialEq)]
pub struct ExecError(String);
impl std::error::Error for ExecError {}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "execution error: {}", self.0)
    }
}

impl std::fmt::Debug for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<String> for ExecError {
    fn from(value: String) -> Self {
        ExecError(value)
    }
}

impl From<&str> for ExecError {
    fn from(value: &str) -> Self {
        ExecError(value.into())
    }
}

pub fn eval(expr: &Expr, schema: &Schema, tuple: &TupleData) -> Result<Value, ExecError> {
    match &expr {
        Expr::Ident(ident) => eval_ident(ident, schema, tuple),
        Expr::Literal(literal) => eval_literal(literal),
        Expr::IsNull { expr, negated } => eval_is_null(expr, *negated, schema, tuple),
        Expr::InList { expr, list, negated } => eval_in_list(expr, list, *negated, schema, tuple),
        Expr::Between { expr, negated, low, high } => {
            eval_between(expr, low, high, *negated, schema, tuple)
        }
        Expr::BinaryOp { left, op, right } => eval_binary_op(left, *op, right, schema, tuple),
        Expr::Function(function) => eval_function(function, schema, tuple),
        Expr::Wildcard => todo!(),
        Expr::QualifiedWildcard(_) => todo!(),
        Expr::SubQuery(_) => todo!(),
    }
}

fn eval_ident(ident: &Ident, schema: &Schema, tuple: &TupleData) -> Result<Value, ExecError> {
    let Some(column) = schema
        .columns
        .iter()
        .find(|Column { name, .. }| name.as_str() == ident.to_string().as_str())
    else {
        Err(format!("unknown identifier: {}", ident))?
    };

    Ok(tuple.get_value(column.offset, column.ty))
}

fn eval_literal(literal: &Literal) -> Result<Value, ExecError> {
    let value = match literal {
        Literal::Number(number) => {
            let int = number.parse::<i32>().map_err(|_| format!("invalid number: {}", number))?;
            Value::Int(int)
        }
        Literal::Decimal(_) => {
            todo!()
        }
        Literal::String(string) => Value::Varchar(string.to_owned()),
        Literal::Bool(bool) => Value::Bool(*bool),
        Literal::Null => todo!(),
    };

    Ok(value)
}

fn eval_is_null(
    expr: &Expr,
    _negated: bool,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, ExecError> {
    let _value = eval(expr, schema, tuple)?;
    todo!("value == null?")
}

fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, ExecError> {
    let search = eval(expr, schema, tuple)?;
    let mut found = false;
    for expr in list {
        let value = eval(expr, schema, tuple)?;
        if value == search {
            found = true;
            break;
        }
    }

    Ok(Value::Bool(found && !negated))
}

fn eval_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, ExecError> {
    let value = eval(expr, schema, tuple)?;
    let low = eval(low, schema, tuple)?;
    let high = eval(high, schema, tuple)?;

    if Value::Bool(true) != value_op(&value, Op::Ge, &low)?
        && Value::Bool(true) != value_op(&value, Op::Le, &high)?
    {
        return Ok(Value::Bool(false));
    }

    Ok(Value::Bool(!negated))
}

fn eval_binary_op(
    lhs: &Expr,
    op: Op,
    rhs: &Expr,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, ExecError> {
    let lhs = eval(lhs, schema, tuple)?;
    let rhs = eval(rhs, schema, tuple)?;

    value_op(&lhs, op, &rhs)
}

fn eval_function(
    function: &Function,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, ExecError> {
    match &function.name {
        f @ FunctionName::Min
        | f @ FunctionName::Max
        | f @ FunctionName::Sum
        | f @ FunctionName::Avg
        | f @ FunctionName::Count => Err(format!("unsupported function: {f}"))?,
        FunctionName::Contains => eval_contains(&function.args, schema, tuple),
        FunctionName::Concat => eval_concat(&function.args, schema, tuple),
    }
}

fn eval_concat(args: &[Expr], schema: &Schema, tuple: &TupleData) -> Result<Value, ExecError> {
    let mut result = String::new();
    for arg in args {
        let value = eval(arg, schema, tuple)?;
        match value {
            Value::TinyInt(v) => result.push_str(&v.to_string()),
            Value::Bool(v) => result.push_str(&v.to_string()),
            Value::Int(v) => result.push_str(&v.to_string()),
            Value::BigInt(v) => result.push_str(&v.to_string()),
            Value::Varchar(v) => result.push_str(&v),
        }
    }

    Ok(Value::Varchar(result))
}

fn eval_contains(args: &[Expr], schema: &Schema, tuple: &TupleData) -> Result<Value, ExecError> {
    if args.len() > 2 {
        Err(format!("CONTAINS expected 2 arguments but got {}", args.len()))?
    }

    let arg0 = eval(&args[0], schema, tuple)?;
    let arg1 = eval(&args[1], schema, tuple)?;

    if arg0.ty() != Type::Varchar && arg1.ty() != Type::Varchar {
        return Ok(Value::Bool(false));
    }

    Ok(Value::Bool(get_value!(arg0, Varchar).contains(get_value!(arg1, Varchar).as_str())))
}

fn value_op(lhs: &Value, op: Op, rhs: &Value) -> Result<Value, ExecError> {
    if lhs.ty() != rhs.ty() {
        Err(format!("cannot perform {} {op} {}", lhs.ty(), rhs.ty()))?
    }

    let result = match lhs {
        Value::TinyInt(lhs) => numeric_op(*lhs, op, *get_value!(rhs, TinyInt)),
        Value::Bool(lhs) => bool_op(*lhs, op, *get_value!(rhs, Bool)),
        Value::Int(lhs) => numeric_op(*lhs, op, *get_value!(rhs, Int)),
        Value::BigInt(lhs) => numeric_op(*lhs, op, *get_value!(rhs, BigInt)),
        Value::Varchar(lhs) => varchar_op(lhs, op, get_value!(rhs, Varchar).as_str()),
    }?;

    Ok(Value::Bool(result))
}

fn numeric_op<T: PartialEq + PartialOrd>(lhs: T, op: Op, rhs: T) -> Result<bool, ExecError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
        Op::And | Op::Or => Err(format!("unsupported numeric operation: {op}"))?,
    };

    Ok(result)
}

fn bool_op(lhs: bool, op: Op, rhs: bool) -> Result<bool, ExecError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => !lhs & rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs & !rhs,
        Op::Ge => lhs >= rhs,
        Op::And => lhs && rhs,
        Op::Or => lhs || rhs,
    };

    Ok(result)
}

fn varchar_op(lhs: &str, op: Op, rhs: &str) -> Result<bool, ExecError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
        Op::And | Op::Or => Err(format!("unsupported string operation: {op}"))?,
    };

    Ok(result)
}

#[cfg(test)]
mod test {
    use super::eval;
    use crate::sql::expr_builder::{concat, contains, ident, lit};
    use crate::table::tuple::{Builder as TupleBuilder, Value};
    use crate::{column, schema};

    macro_rules! test_eval {
        ($name:tt, $expr:expr, $schema:expr, $tuple:expr, $want:expr) => {
            #[test]
            fn $name() {
                let have = eval(&$expr, &$schema, &$tuple);
                assert_eq!($want, have);
            }
        };
        ($name:tt, $expr:expr, $want:expr) => {
            #[test]
            fn $name() {
                let have = eval(&$expr, &crate::schema! {}, &TupleBuilder::new().build());
                assert_eq!($want, have);
            }
        };
    }

    test_eval!(t1, lit("test").eq(lit("test")), Ok(Value::Bool(true)));
    test_eval!(t2, lit(22).eq(lit(23)), Ok(Value::Bool(false)));
    test_eval!(t3, lit(22).lt(lit(23)), Ok(Value::Bool(true)));
    test_eval!(t4, lit(22).and(lit(23)), Err("unsupported numeric operation: AND".into()));
    test_eval!(t5, ident("c1"), Err("unknown identifier: c1".into()));

    test_eval!(
        t6,
        ident("c1"),
        schema! {column!("c1", Int)},
        TupleBuilder::new().int(1).build(),
        Ok(Value::Int(1))
    );

    test_eval!(
        t7,
        ident("c1").eq(lit(1)),
        schema! {column!("c1", Int)},
        TupleBuilder::new().int(1).build(),
        Ok(Value::Bool(true))
    );

    test_eval!(
        t8,
        ident("c1").eq(lit("1")),
        schema! {column!("c1", Int)},
        TupleBuilder::new().int(1).build(),
        Err("cannot perform INT = VARCHAR".into())
    );

    test_eval!(
        t9,
        ident("c1").eq(lit("a")).and(ident("c2").between(lit(20), lit(30))),
        schema! {column!("c1", Varchar), column!("c2", Int)},
        TupleBuilder::new().varchar("a").int(20).build(),
        Ok(Value::Bool(true))
    );

    test_eval!(
        t10,
        contains(vec![ident("c1"), lit("sd")]),
        schema! {column!("c1", Varchar)},
        TupleBuilder::new().varchar("asdf").build(),
        Ok(Value::Bool(true))
    );

    test_eval!(
        t11,
        concat(vec![ident("c1"), ident("c2"), lit("c"), lit(9)]),
        schema! {column!("c1", Varchar), column!("c2", Varchar)},
        TupleBuilder::new().varchar("a").varchar("b").build(),
        Ok(Value::Varchar("abc9".to_string()))
    );

    test_eval!(
        t12,
        concat(vec![concat(vec![ident("c1"), ident("c2")]), concat(vec![lit("c"), lit(9)])]),
        schema! {column!("c1", Varchar), column!("c2", Varchar)},
        TupleBuilder::new().varchar("a").varchar("b").build(),
        Ok(Value::Varchar("abc9".to_string()))
    );
}
