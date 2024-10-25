use crate::{
    catalog::{Column, Schema, Type},
    get_value,
    logical_plan::{Expr, Function, FunctionName, Op, Value as Literal},
    table::tuple::{TupleData, Value},
};

enum EvalError {
    UnknownIdentifier,
    UnsupportedOperation,
    UnsupportedFunction,
}
use EvalError::*;

fn eval(expr: &Expr, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    match &expr {
        Expr::Ident(ident) => eval_ident(ident, schema, tuple),
        Expr::Value(literal) => eval_literal(literal, schema, tuple),
        Expr::IsNull(expr) => eval_is_null(expr, schema, tuple),
        Expr::IsNotNull(expr) => eval_is_not_null(expr, schema, tuple),
        Expr::InList { expr, list, negated } => eval_in_list(expr, list, *negated, schema, tuple),
        Expr::Between { expr, negated, low, high } => {
            eval_between(expr, low, high, *negated, schema, tuple)
        }
        Expr::BinaryOp { left, op, right } => eval_binary_op(left, *op, right, schema, tuple),
        Expr::Function(function) => eval_function(function),
    }
}

fn eval_ident(ident: &str, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    let Some(column) = schema.columns.iter().find(|Column { name, .. }| name == ident) else {
        Err(UnknownIdentifier)?
    };
    Ok(tuple.get_value(column))
}

fn eval_literal(literal: &Literal, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    match literal {
        Literal::Number(_) => todo!(),
        Literal::String(_) => todo!(),
        Literal::Bool(_) => todo!(),
        Literal::Null => todo!(),
    }
}

fn eval_is_null(expr: &Expr, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    let value = eval(expr, schema, tuple)?;
    todo!("value == null?")
}

fn eval_is_not_null(expr: &Expr, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    let value = eval(expr, schema, tuple)?;
    todo!("value != null?")
}

fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, EvalError> {
    let search = eval(expr, schema, tuple)?;
    let mut found = false;
    for expr in list {
        let value = eval(expr, schema, tuple)?;
        if value == search {
            found = true;
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
) -> Result<Value, EvalError> {
    todo!()
}

fn eval_binary_op(
    lhs: &Expr,
    op: Op,
    rhs: &Expr,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, EvalError> {
    macro_rules! get_value {
        ($value:ident, $type:tt) => {
            match $value {
                Value::$type(value) => value,
                _ => unreachable!(),
            }
        };
    }

    let lhs = eval(lhs, schema, tuple)?;
    let rhs = eval(rhs, schema, tuple)?;

    if Type::from(&lhs) != Type::from(&rhs) {
        Err(UnsupportedOperation)?
    }

    // TODO: currently just supports comparisons but should support math too
    let result = match lhs {
        Value::TinyInt(lhs) => tiny_int_op(lhs, op, get_value!(rhs, TinyInt)),
        Value::Bool(lhs) => bool_op(lhs, op, get_value!(rhs, Bool)),
        Value::Int(lhs) => int_op(lhs, op, get_value!(rhs, Int)),
        Value::BigInt(lhs) => big_int_op(lhs, op, get_value!(rhs, BigInt)),
        Value::Varchar(lhs) => varchar_op(&lhs, op, get_value!(rhs, Varchar).as_str()),
    }?;

    Ok(Value::Bool(result))
}

fn eval_function(function: &Function) -> Result<Value, EvalError> {
    match function.name {
        FunctionName::Min
        | FunctionName::Max
        | FunctionName::Sum
        | FunctionName::Avg
        | FunctionName::Count => Err(UnsupportedFunction),
    }
}

fn tiny_int_op(lhs: i8, op: Op, rhs: i8) -> Result<bool, EvalError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
        Op::And | Op::Or => Err(UnsupportedOperation)?,
    };

    Ok(result)
}

fn bool_op(lhs: bool, op: Op, rhs: bool) -> Result<bool, EvalError> {
    let result = match op {
        Op::Eq | Op::Neq | Op::Lt | Op::Le | Op::Gt | Op::Ge => Err(UnsupportedOperation)?,
        Op::And => lhs && rhs,
        Op::Or => lhs || rhs,
    };

    Ok(result)
}

fn int_op(lhs: i32, op: Op, rhs: i32) -> Result<bool, EvalError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
        Op::And | Op::Or => Err(UnsupportedOperation)?,
    };

    Ok(result)
}

fn big_int_op(lhs: i64, op: Op, rhs: i64) -> Result<bool, EvalError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
        Op::And | Op::Or => Err(UnsupportedOperation)?,
    };

    Ok(result)
}

fn varchar_op(lhs: &str, op: Op, rhs: &str) -> Result<bool, EvalError> {
    let result = match op {
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt | Op::Le | Op::Gt | Op::Ge | Op::And | Op::Or => Err(UnsupportedOperation)?,
    };

    Ok(result)
}
