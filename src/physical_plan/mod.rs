use crate::{
    catalog::{Column, Schema, Type},
    get_value,
    logical_plan::expr::{Expr, Function, FunctionName, Op, Value as Literal},
    table::tuple::{Data as TupleData, Value},
};

macro_rules! get_value {
    ($value:ident, $type:tt) => {
        match $value {
            Value::$type(value) => value,
            _ => unreachable!(),
        }
    };
}

#[derive(Debug, PartialEq)]
enum EvalError {
    UnknownIdentifier,
    UnsupportedOperation,
    UnsupportedFunction,
    InvalidNumber,
    IncorrectNumberOfArguments,
}
use EvalError::*;

fn eval(expr: &Expr, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    match &expr {
        Expr::Ident(ident) => eval_ident(ident, schema, tuple),
        Expr::Value(literal) => eval_literal(literal),
        Expr::IsNull(expr) => eval_is_null(expr, schema, tuple),
        Expr::IsNotNull(expr) => eval_is_not_null(expr, schema, tuple),
        Expr::InList { expr, list, negated } => eval_in_list(expr, list, *negated, schema, tuple),
        Expr::Between { expr, negated, low, high } => {
            eval_between(expr, low, high, *negated, schema, tuple)
        }
        Expr::BinaryOp { left, op, right } => eval_binary_op(left, *op, right, schema, tuple),
        Expr::Function(function) => eval_function(function, schema, tuple),
    }
}

fn eval_ident(ident: &str, schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    let Some(column) = schema.columns.iter().find(|Column { name, .. }| name == ident) else {
        Err(UnknownIdentifier)?
    };

    Ok(tuple.get_value(column))
}

fn eval_literal(literal: &Literal) -> Result<Value, EvalError> {
    let value = match literal {
        Literal::Number(number) => {
            let int = number.parse::<i32>().map_err(|_| InvalidNumber)?;
            Value::Int(int)
        }
        Literal::String(string) => Value::Varchar(string.to_owned()),
        Literal::Bool(bool) => Value::Bool(*bool),
        Literal::Null => todo!(),
    };

    Ok(value)
}

// TODO: implement these once NULL has been implemented
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
) -> Result<Value, EvalError> {
    let value = eval(expr, schema, tuple)?;
    let low = eval(low, schema, tuple)?;
    let high = eval(high, schema, tuple)?;

    if Value::Bool(true) != value_op(&value, Op::Ge, &low)?
        && Value::Bool(true) != value_op(&value, Op::Le, &high)?
    {
        return Ok(Value::Bool(false));
    }

    Ok(Value::Bool(true && !negated))
}

fn eval_binary_op(
    lhs: &Expr,
    op: Op,
    rhs: &Expr,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, EvalError> {
    let lhs = eval(lhs, schema, tuple)?;
    let rhs = eval(rhs, schema, tuple)?;

    value_op(&lhs, op, &rhs)
}

fn eval_function(
    function: &Function,
    schema: &Schema,
    tuple: &TupleData,
) -> Result<Value, EvalError> {
    match function.name {
        FunctionName::Min
        | FunctionName::Max
        | FunctionName::Sum
        | FunctionName::Avg
        | FunctionName::Count => Err(UnsupportedFunction),
        FunctionName::Contains => eval_contains(&function.args, schema, tuple),
        FunctionName::Concat => eval_concat(&function.args, schema, tuple),
    }
}

fn eval_concat(args: &[Expr], schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
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

fn eval_contains(args: &[Expr], schema: &Schema, tuple: &TupleData) -> Result<Value, EvalError> {
    if args.len() > 2 {
        Err(IncorrectNumberOfArguments)?
    }

    let arg0 = eval(&args[0], schema, tuple)?;
    let arg1 = eval(&args[1], schema, tuple)?;

    if arg0.ty() != Type::Varchar && arg1.ty() != Type::Varchar {
        return Ok(Value::Bool(false));
    }

    Ok(Value::Bool(get_value!(arg0, Varchar).contains(get_value!(arg1, Varchar).as_str())))
}

fn value_op(lhs: &Value, op: Op, rhs: &Value) -> Result<Value, EvalError> {
    if lhs.ty() != rhs.ty() {
        Err(UnsupportedOperation)?
    }

    // TODO: currently just supports comparisons but can support math too
    let result = match lhs {
        Value::TinyInt(lhs) => tiny_int_op(*lhs, op, *get_value!(rhs, TinyInt)),
        Value::Bool(lhs) => bool_op(*lhs, op, *get_value!(rhs, Bool)),
        Value::Int(lhs) => int_op(*lhs, op, *get_value!(rhs, Int)),
        Value::BigInt(lhs) => big_int_op(*lhs, op, *get_value!(rhs, BigInt)),
        Value::Varchar(lhs) => varchar_op(&lhs, op, get_value!(rhs, Varchar).as_str()),
    }?;

    Ok(Value::Bool(result))
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
        Op::Eq => lhs == rhs,
        Op::Neq => lhs != rhs,
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
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
        Op::Lt => lhs < rhs,
        Op::Le => lhs <= rhs,
        Op::Gt => lhs > rhs,
        Op::Ge => lhs >= rhs,
        Op::And | Op::Or => Err(UnsupportedOperation)?,
    };

    Ok(result)
}

#[cfg(test)]
mod test {
    use {
        super::{eval, EvalError::*},
        crate::{
            catalog::Type,
            logical_plan::expr::{concat, contains, ident, number, string},
            table::tuple::{Builder as TupleBuilder, Value},
        },
    };

    macro_rules! test_eval {
        ($name:tt, $expr:expr, $schema:expr, $tuple:expr, $want:expr) => {
            #[test]
            fn $name() {
                let have = eval(&$expr, &$schema.into(), &$tuple);
                assert_eq!($want, have);
            }
        };
        ($name:tt, $expr:expr, $want:expr) => {
            #[test]
            fn $name() {
                let have = eval(&$expr, &[].into(), &TupleBuilder::new().build());
                assert_eq!($want, have);
            }
        };
    }

    test_eval!(t1, string("test").eq(string("test")), Ok(Value::Bool(true)));
    test_eval!(t2, number("22").eq(number("23")), Ok(Value::Bool(false)));
    test_eval!(t3, number("22").lt(number("23")), Ok(Value::Bool(true)));
    test_eval!(t4, number("22").and(number("23")), Err(UnsupportedOperation));
    test_eval!(t5, ident("c1"), Err(UnknownIdentifier));

    test_eval!(
        t6,
        ident("c1"),
        [("c1", Type::Int)],
        TupleBuilder::new().int(1).build(),
        Ok(Value::Int(1))
    );

    test_eval!(
        t7,
        ident("c1").eq(number("1")),
        [("c1", Type::Int)],
        TupleBuilder::new().int(1).build(),
        Ok(Value::Bool(true))
    );

    test_eval!(
        t8,
        ident("c1").eq(string("1")),
        [("c1", Type::Int)],
        TupleBuilder::new().int(1).build(),
        Err(UnsupportedOperation)
    );

    test_eval!(
        t9,
        ident("c1").eq(string("a")).and(ident("c2").between(number("20"), number("30"))),
        [("c1", Type::Varchar), ("c2", Type::Int)],
        TupleBuilder::new().varchar("a").int(20).build(),
        Ok(Value::Bool(true))
    );

    test_eval!(
        t10,
        contains(vec![ident("c1"), string("sd")]),
        [("c1", Type::Varchar)],
        TupleBuilder::new().varchar("asdf").build(),
        Ok(Value::Bool(true))
    );

    test_eval!(
        t11,
        concat(vec![ident("c1"), ident("c2"), string("c"), number("9")]),
        [("c1", Type::Varchar), ("c2", Type::Varchar)],
        TupleBuilder::new().varchar("a").varchar("b").build(),
        Ok(Value::Varchar("abc9".to_string()))
    );

    test_eval!(
        t12,
        concat(vec![
            concat(vec![ident("c1"), ident("c2")]),
            concat(vec![string("c"), number("9")])
        ]),
        [("c1", Type::Varchar), ("c2", Type::Varchar)],
        TupleBuilder::new().varchar("a").varchar("b").build(),
        Ok(Value::Varchar("abc9".to_string()))
    );
}
