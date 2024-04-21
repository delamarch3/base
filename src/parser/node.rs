use std::rc::Rc;

use super::lexer::Token;

#[derive(Debug, PartialEq)]
pub enum Op {
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,

    Conjunction,
    Disjunction,

    Negation,
    In,
    Between,
    Is,

    NotIn,
    NotBetween,
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
            Op::Conjunction => write!(f, "AND"),
            Op::Disjunction => write!(f, "OR"),
            Op::Negation => write!(f, "NOT"),
            Op::In => write!(f, "IN"),
            Op::Between => write!(f, "BETWEEN"),
            Op::Is => write!(f, "IS"),
            Op::NotIn => write!(f, "NOT IN"),
            Op::NotBetween => write!(f, "NOT BETWEEN"),
        }
    }
}

impl From<Token> for Op {
    fn from(t: Token) -> Self {
        match t {
            Token::Conjunction => Op::Conjunction,
            Token::Disjunction => Op::Disjunction,
            Token::Eq => Op::Eq,
            Token::Neq => Op::Neq,
            Token::Lt => Op::Lt,
            Token::Le => Op::Le,
            Token::Gt => Op::Gt,
            Token::Ge => Op::Ge,
            Token::Negation => Op::Negation,
            Token::In => Op::In,
            Token::Between => Op::Between,
            Token::Is => Op::Is,

            _ => unreachable!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Type {
    Int,
    Varchar(Box<Node>),
}

impl TryFrom<Token> for Type {
    type Error = Unexpected;

    fn try_from(t: Token) -> std::prelude::v1::Result<Self, Self::Error> {
        match t {
            Token::Int => Ok(Type::Int),
            Token::Varchar => Ok(Type::Varchar(Box::new(Node::None))),
            t => Err(Unexpected(t)),
        }
    }
}

// TODO: Support more join types
#[derive(Debug, PartialEq)]
pub enum JoinType {
    None,
    Inner,
}

#[derive(Debug, PartialEq)]
pub enum Node {
    Select {
        projection: Vec<Node>,
        from: Box<Node>,           // TableRef|Select
        filter: Option<Box<Node>>, // Expr
        group: Vec<Node>,
        order: Vec<Node>,
        limit: Option<Box<Node>>,
    },

    Insert {
        columns: Vec<Node>,      // ColumnRef
        table: Box<Node>,        // TableRef
        inserts: Vec<Vec<Node>>, // [[StringLiteral|IntegerLiteral|Null]]
    },

    Create {
        table: String,
        columns: Vec<Node>, // ColumnDef
    },

    Delete {
        table: String,
        filter: Option<Box<Node>>, // Expr
        limit: Option<Box<Node>>,
    },

    Update {
        table: String,
        assignments: Vec<Node>,
        filter: Option<Box<Node>>,
    },

    Assignment {
        column: String,
        value: Box<Node>,
    },

    ColumnDef {
        column: String,
        ty: Type,
        // TODO: constraints
    },

    Expr(Op, Vec<Node>),

    ColumnRef {
        table: Option<String>,
        column: String,
        alias: Option<String>,
    },

    TableRef(String),

    From {
        ty: JoinType,
        left: Box<Node>,           // TableRef|Select
        right: Box<Node>,          // TableRef|Select
        using: Option<Vec<Node>>,  // ColumnRef
        filter: Option<Box<Node>>, // Expr
        alias: Option<String>,
    },

    StringLiteral(String),
    IntegerLiteral(u64),

    Between(Box<Node>, Box<Node>),
    In(Vec<Node>),

    All,
    Null,

    None,
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Node::Expr(operator, operands) => {
                write!(f, "({}", operator)?;
                for o in operands {
                    write!(f, " {}", o)?;
                }
                write!(f, ")")
            }
            Node::ColumnRef {
                table,
                column,
                alias,
            } => {
                if table.is_some() {
                    write!(f, "{}.", table.as_ref().unwrap())?;
                }

                write!(f, "{column}")?;

                if alias.is_some() {
                    write!(f, "AS {}", alias.as_ref().unwrap())?;
                }

                Ok(())
            }
            Node::TableRef(t) => write!(f, "{t}"),
            Node::StringLiteral(s) => write!(f, "\"{s}\""),
            Node::IntegerLiteral(i) => write!(f, "{i}"),
            Node::All => write!(f, "ALL"),
            Node::Null => write!(f, "NULL"),
            Node::None => write!(f, "NONE"),
            Node::Between(from, to) => write!(f, "({} {})", from, to),
            Node::In(vs) => {
                if vs.len() == 0 {
                    return write!(f, "[]");
                }

                write!(f, "[{}", vs[0])?;
                for v in &vs[1..] {
                    write!(f, " {}", v)?;
                }
                write!(f, "]")
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(PartialEq)]
pub struct Unexpected(pub Token);

impl std::fmt::Display for Unexpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unexpected token: {:?}", self.0)
    }
}

impl std::fmt::Debug for Unexpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unexpected token: {:?}", self.0)
    }
}

impl std::error::Error for Unexpected {}

pub type Result<T> = std::result::Result<T, Unexpected>;

#[macro_export]
macro_rules! check_next {
    ($l:ident, :$want:path) => {
        match $l.peek() {
            $want => {$l.next();}
            _ => {},
        }
    };
    ($l:ident, $need:path) => {
        match $l.next() {
            $need => {}
            t => Err(Unexpected(t))?,
        }
    };
    ($l:ident, [$( $($need:path)? $(: $want:path)*),*]) => {
        $(
            $(
                crate::check_next!($l, :$want);
            )*

            $(
                crate::check_next!($l, $need);
            )?
        )*
    };
}
