use self::lexer::{Lexer, Token};

mod lexer;

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

impl TryFrom<&str> for Node {
    type Error = Unexpected;

    fn try_from(input: &str) -> std::prelude::v1::Result<Self, Self::Error> {
        parse(&mut Lexer::new(input))
    }
}

// TODO: Support unions
fn parse(l: &mut Lexer) -> Result<Node> {
    let q = match l.peek() {
        Token::Create => create(l),
        Token::Select => select(l),
        Token::Insert => insert(l),
        Token::Update => update(l),
        Token::Delete => delete(l),
        t => Err(Unexpected(t)),
    };

    match l.next() {
        Token::Semicolon => q,
        t => Err(Unexpected(t)),
    }
}

pub fn select(l: &mut Lexer) -> Result<Node> {
    check_next!(l, Token::Select);
    let projection = projection(l)?;
    check_next!(l, Token::From);

    let mut from = Box::new(Node::From {
        ty: JoinType::None,
        left: Box::new(table_ref(l)?),
        right: Box::new(Node::None),
        using: None,
        filter: None,
        alias: None,
    });
    let mut filter = None;
    let mut group = vec![];
    let mut order = vec![];
    let mut limit = None;
    loop {
        match l.peek() {
            Token::Join => {
                if filter.is_some() || !group.is_empty() || !order.is_empty() || limit.is_some() {
                    Err(Unexpected(l.next()))?
                }

                join_tables(l, &mut from)?;
            }
            Token::Where => {
                if filter.is_some() || !group.is_empty() || !order.is_empty() || limit.is_some() {
                    Err(Unexpected(l.next()))?
                }

                filter = Some(Box::new(where_expr(l)?))
            }
            Token::Group => {
                if !group.is_empty() || !order.is_empty() || limit.is_some() {
                    Err(Unexpected(l.next()))?
                }

                group = group_expr(l)?;
            }
            Token::Order => {
                if !order.is_empty() || limit.is_some() {
                    Err(Unexpected(l.next()))?
                }

                order = order_expr(l)?;
            }
            Token::Limit => {
                if limit.is_some() {
                    Err(Unexpected(l.next()))?
                }

                limit = Some(Box::new(limit_expr(l)?));
            }
            _ => break, // `parens` will check RParen, `select` will check Semicolon
        };
    }

    Ok(Node::Select {
        projection,
        from,
        filter,
        group,
        order,
        limit,
    })
}

pub fn insert(l: &mut Lexer) -> Result<Node> {
    check_next!(l, [Token::Insert, :Token::Into]);

    let table = match l.next() {
        Token::TableOrColumnReference(table) => Box::new(Node::TableRef(table)),
        t => Err(Unexpected(t))?,
    };

    let columns = match l.peek() {
        Token::LParen => parens(list(column_ref))(l)?,
        _ => vec![],
    };

    check_next!(l, Token::Values);

    let inserts = list(parens(list(literal)))(l)?;

    Ok(Node::Insert {
        columns,
        table,
        inserts,
    })
}

pub fn delete(l: &mut Lexer) -> Result<Node> {
    check_next!(l, [Token::Delete, Token::From]);

    let table = match l.next() {
        Token::TableOrColumnReference(table) => table,
        t => Err(Unexpected(t))?,
    };

    let mut filter = None;
    if let Token::Where = l.peek() {
        filter = Some(Box::new(where_expr(l)?));
    }

    let mut limit = None;
    if let Token::Limit = l.peek() {
        limit = Some(Box::new(limit_expr(l)?));
    }

    Ok(Node::Delete {
        table,
        filter,
        limit,
    })
}

pub fn update(l: &mut Lexer) -> Result<Node> {
    check_next!(l, Token::Update);

    let table = match l.next() {
        Token::TableOrColumnReference(table) => table,
        t => Err(Unexpected(t))?,
    };

    check_next!(l, Token::Set);

    let assignments = list(assignment)(l)?;

    let mut filter = None;
    if let Token::Where = l.peek() {
        filter = Some(Box::new(where_expr(l)?));
    }

    Ok(Node::Update {
        table,
        assignments,
        filter,
    })
}

fn assignment(l: &mut Lexer) -> Result<Node> {
    let column = match l.next() {
        Token::TableOrColumnReference(column) => column,
        t => Err(Unexpected(t))?,
    };

    check_next!(l, Token::Eq);

    let value = Box::new(literal(l)?);

    Ok(Node::Assignment { column, value })
}

pub fn create(l: &mut Lexer) -> Result<Node> {
    check_next!(l, [Token::Create, Token::Table]);

    let table = match l.next() {
        Token::TableOrColumnReference(table) => table,
        t => Err(Unexpected(t))?,
    };

    let columns = parens(list(column_def))(l)?;

    Ok(Node::Create { table, columns })
}

// TODO: should parse expressions
fn projection(l: &mut Lexer) -> Result<Vec<Node>> {
    match l.peek() {
        Token::All => {
            l.next();
            return Ok(vec![Node::All]);
        }

        _ => list(column_ref)(l),
    }
}

fn list<T>(
    f: impl Fn(&mut Lexer) -> Result<T> + Copy,
) -> impl Fn(&mut Lexer) -> Result<Vec<T>> + Copy {
    move |l: &mut Lexer| -> Result<Vec<T>> {
        #[derive(PartialEq, Debug)]
        enum State {
            Comma,
            Item,
        }

        fn _list<T>(
            l: &mut Lexer,
            f: impl Fn(&mut Lexer) -> Result<T>,
            mut list: Vec<T>,
            state: State,
        ) -> Result<Vec<T>> {
            match l.peek() {
                Token::Comma if state == State::Comma => {
                    l.next();
                    _list(l, f, list, State::Item)
                }
                _ if state == State::Comma => return Ok(list),
                _ => {
                    list.push(f(l)?);
                    _list(l, f, list, State::Comma)
                }
            }
        }

        _list(l, f, Vec::new(), State::Item)
    }
}

fn integer(l: &mut Lexer) -> Result<Node> {
    match l.next() {
        Token::IntegerLiteral(i) => Ok(Node::IntegerLiteral(i)),
        t => Err(Unexpected(t)),
    }
}

fn string(l: &mut Lexer) -> Result<Node> {
    match l.next() {
        Token::StringLiteral(s) => Ok(Node::StringLiteral(s)),
        t => Err(Unexpected(t)),
    }
}

fn null(l: &mut Lexer) -> Result<Node> {
    match l.next() {
        Token::Null => Ok(Node::Null),
        t => Err(Unexpected(t)),
    }
}

fn literal(l: &mut Lexer) -> Result<Node> {
    match l.peek() {
        Token::StringLiteral(_) => string(l),
        Token::IntegerLiteral(_) => integer(l),
        Token::Null => null(l),
        t => Err(Unexpected(t)),
    }
}

fn column_ref(l: &mut Lexer) -> Result<Node> {
    match l.next() {
        Token::TableOrColumnReference(column) => Ok(Node::ColumnRef {
            table: None,
            column,
            alias: None,
        }),

        Token::TableAndColumnReference(table, column) => Ok(Node::ColumnRef {
            table: Some(table),
            column,
            alias: None,
        }),

        t => Err(Unexpected(t)),
    }
}

fn table_ref(l: &mut Lexer) -> Result<Node> {
    match l.peek() {
        Token::TableOrColumnReference(table) => {
            l.next();
            Ok(Node::TableRef(table))
        }
        Token::LParen => parens(select)(l),
        t => Err(Unexpected(t)),
    }
}

// select * from table_a
// join table_b using (column_a)
// join table_c using (column_c)
// join table_d using (column_a)
// ->
//     | JOIN using column_a
//     |- table_d
//     | JOIN using column_c
//     |- table_c
//     | JOIN using column_a
//     |- table_b
//     table_a
fn join_tables(l: &mut Lexer, left: &mut Box<Node>) -> Result<()> {
    if l.peek() != Token::Join {
        return Ok(());
    }
    l.next();

    let right_table = Box::new(table_ref(l)?);
    let mut using_cond = None;
    let mut on_cond = None;

    match l.next() {
        Token::Using => {
            using_cond = Some(parens(list(column_ref))(l)?);
            // TODO: fill out expr_
        }
        Token::On => on_cond = Some(Box::new(parens(expr)(l)?)),
        t => Err(Unexpected(t))?,
    };

    match left.as_mut() {
        Node::From {
            ty,
            right,
            using,
            filter: expr,
            alias,
            ..
        } => {
            *ty = JoinType::Inner;
            *right = Box::new(Node::From {
                ty: JoinType::None,
                left: right_table,
                right: Box::new(Node::None),
                using: None,
                filter: None,
                alias: None,
            });
            *alias = None;
            *using = using_cond;
            *expr = on_cond;

            join_tables(l, right)
        }
        _ => unreachable!(),
    }
}

fn where_expr(l: &mut Lexer) -> Result<Node> {
    check_next!(l, Token::Where);

    expr(l)
}

fn group_expr(l: &mut Lexer) -> Result<Vec<Node>> {
    check_next!(l, [Token::Group, Token::By]);

    list(column_ref)(l)
}

fn order_expr(l: &mut Lexer) -> Result<Vec<Node>> {
    check_next!(l, [Token::Order, Token::By]);

    list(column_ref)(l)
}

fn limit_expr(l: &mut Lexer) -> Result<Node> {
    check_next!(l, Token::Limit);

    match l.next() {
        Token::IntegerLiteral(i) => Ok(Node::IntegerLiteral(i)),
        t => Err(Unexpected(t)),
    }
}

fn parens<T>(
    f: impl Fn(&mut Lexer) -> Result<T> + Copy,
) -> impl Fn(&mut Lexer) -> Result<T> + Copy {
    move |l: &mut Lexer| -> Result<T> {
        check_next!(l, Token::LParen);
        let n = f(l)?;
        check_next!(l, Token::RParen);

        Ok(n)
    }
}

fn column_def(l: &mut Lexer) -> Result<Node> {
    let column = match l.next() {
        Token::TableOrColumnReference(column) => column,
        t => Err(Unexpected(t))?,
    };

    let mut ty = l.next().try_into()?;
    if let Type::Varchar(n) = &mut ty {
        *n.as_mut() = parens(integer)(l)?
    }

    Ok(Node::ColumnDef { column, ty })
}

fn expr(l: &mut Lexer) -> Result<Node> {
    #[derive(PartialEq, Debug, Clone, Copy)]
    enum State {
        None,
        Between,
        In,
    }

    fn expr_bp(l: &mut Lexer, min_bp: u8, mut state: State) -> Result<Node> {
        let mut lhs: Node = match state {
            State::Between => between(l)?,
            State::In => Node::In(parens(list(literal))(l)?),
            State::None => match l.next() {
                Token::TableAndColumnReference(table, column) => Node::ColumnRef {
                    table: Some(table),
                    column,
                    alias: None,
                },
                Token::TableOrColumnReference(column) => Node::ColumnRef {
                    table: None,
                    column,
                    alias: None,
                },
                Token::StringLiteral(s) => Node::StringLiteral(s),
                Token::IntegerLiteral(i) => Node::IntegerLiteral(i),
                Token::Null => Node::Null,
                t => Err(Unexpected(t))?,
            },
        };

        loop {
            let op = match l.peek() {
                // some operators can consist of multiple tokens
                Token::Negation => {
                    l.next();
                    let op = match l.peek() {
                        t if is_infix(&t) => t.into(),
                        t => Err(Unexpected(t))?,
                    };

                    match op {
                        Op::Between => Op::NotBetween,
                        Op::In => Op::NotIn,
                        _ => Err(Unexpected(l.peek()))?,
                    }
                }
                Token::Is => match l.peek_n(2) {
                    Token::Negation => {
                        l.next();
                        Op::Negation
                    }
                    _ => Op::Is,
                },
                t if is_infix(&t) => t.into(),
                _ => break,
            };

            // change the state if needed to ensure operands are parsed correctly
            match op {
                Op::Between | Op::NotBetween => state = State::Between,
                Op::In | Op::NotIn => state = State::In,
                _ => state = State::None,
            }

            let (l_bp, r_bp) = infix_bp(&op);
            if l_bp < min_bp {
                break;
            }

            let rhs = match l.next() {
                _ => expr_bp(l, r_bp, state)?,
            };

            lhs = Node::Expr(op, vec![lhs, rhs]);
        }

        Ok(lhs)
    }

    expr_bp(l, 0, State::None)
}

fn between(l: &mut Lexer) -> Result<Node> {
    let from = match l.next() {
        Token::IntegerLiteral(i) => Node::IntegerLiteral(i),
        t => Err(Unexpected(t))?,
    };

    check_next!(l, Token::Conjunction);

    let to = match l.next() {
        Token::IntegerLiteral(i) => Node::IntegerLiteral(i),
        t => Err(Unexpected(t))?,
    };

    Ok(Node::Between(Box::new(from), Box::new(to)))
}

fn is_infix(t: &Token) -> bool {
    match t {
        Token::Negation
        | Token::Is
        | Token::Conjunction
        | Token::Disjunction
        | Token::Between
        | Token::In
        | Token::Eq
        | Token::Neq
        | Token::Lt
        | Token::Le
        | Token::Gt
        | Token::Ge => true,
        _ => false,
    }
}

fn infix_bp(op: &Op) -> (u8, u8) {
    match op {
        Op::Conjunction | Op::Disjunction => (1, 2),
        Op::Eq | Op::Neq | Op::Lt | Op::Le | Op::Gt | Op::Ge => (3, 4),
        Op::Negation | Op::Is => (3, 4),
        Op::In | Op::Between => (5, 6),
        Op::NotIn | Op::NotBetween => (5, 6),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::parser::Type;

    #[test]
    fn test_parse_expr() -> Result<()> {
        struct Test {
            input: &'static str,
            want: &'static str,
        }

        let tcs = [
            Test {
                input: "12 = 12",
                want: "(= 12 12)",
            },
            Test {
                input: "12 < 14 AND 14 > 12",
                want: "(AND (< 12 14) (> 14 12))",
            },
            Test {
                input: "12 < 14 AND 14 > 12 OR name != \"bob\"",
                want: "(OR (AND (< 12 14) (> 14 12)) (!= name \"bob\"))",
            },
            Test {
                input: "NULL",
                want: "NULL",
            },
            Test {
                input: "columna BETWEEN 100 AND 200",
                want: "(BETWEEN columna (100 200))",
            },
            Test {
                input: "columna IN (100, 200, 300)",
                want: "(IN columna [100 200 300])",
            },
            Test {
                input: "columna IS NULL",
                want: "(IS columna NULL)",
            },
            Test {
                input: "columna IS NOT NULL",
                want: "(NOT columna NULL)",
            },
            Test {
                input: "columna NOT BETWEEN 100 AND 200",
                want: "(NOT BETWEEN columna (100 200))",
            },
            Test {
                input: "columna BETWEEN 100 AND 200 AND 1 < 2",
                want: "(AND (BETWEEN columna (100 200)) (< 1 2))",
            },
            Test {
                input: "columna NOT BETWEEN 100 AND 200 AND 1 < 2",
                want: "(AND (NOT BETWEEN columna (100 200)) (< 1 2))",
            },
            Test {
                input: "columna NOT IN (1, 2, 3, 4)",
                want: "(NOT IN columna [1 2 3 4])",
            },
            Test {
                input: "columna NOT IN (1, 2, 3, 4) AND columna = columnb OR columna IN (6, 7, 8)",
                want:
                    "(OR (AND (NOT IN columna [1 2 3 4]) (= columna columnb)) (IN columna [6 7 8]))",
            },
        ];

        for Test { input, want } in tcs {
            let mut l = Lexer::new(input);
            let have = expr(&mut l)?.to_string();
            assert_eq!(want, have);
        }

        Ok(())
    }

    struct Test {
        input: &'static str,
        want: Result<Node>,
    }

    #[test]
    fn test_parse_select() -> Result<()> {
        let tcs = [
                Test {
                    input: "select * from tablea;",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(Node::From {
                                ty: JoinType::None,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::None),
                                using: None,
                                filter: None,
                                alias: None,
                        }),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select columna, columnb from tablea;",
                    want: Ok(Node::Select {
                        projection: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None,
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None,
                            },
                        ],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::None,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::None),
                                using: None,
                                filter: None,
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select columna, columnb, columnc from tablea;",
                    want: Ok(Node::Select {
                        projection: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None,
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None,
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnc".into(),
                                alias: None,
                            },
                        ],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::None,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::None),
                                using: None,
                                filter: None,
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea join tableb using (columna, columnb);",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::Inner,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::From {
                                    ty: JoinType::None,
                                    left: Box::new(Node::TableRef("tableb".into())),
                                    right: Box::new(Node::None),
                                    using: None,
                                    filter: None,
                                    alias: None,
                                }),
                                using: Some(vec![
                                    Node::ColumnRef {
                                        table: None,
                                        column: "columna".into(),
                                        alias: None,
                                    },
                                    Node::ColumnRef {
                                        table: None,
                                        column: "columnb".into(),
                                        alias: None,
                                    },
                                ]),
                                filter: None,
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea join (select * from tableb) using (columna, columnb);",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::Inner,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::From {
                                    ty: JoinType::None,
                                    left: Box::new(Node::Select {
                                        projection: vec![Node::All],
                                        from: Box::new(Node::From {
                                                ty: JoinType::None,
                                                left: Box::new(Node::TableRef("tableb".into())),
                                                right: Box::new(Node::None),
                                                using: None,
                                                filter: None,
                                                alias: None,
                                        }),
                                        filter: None,
                                        group: vec![],
                                        order: vec![],
                                        limit: None,
                                    }),
                                    right: Box::new(Node::None),
                                    using: None,
                                    filter: None,
                                    alias: None,
                                }),
                                using: Some(vec![
                                    Node::ColumnRef {
                                        table: None,
                                        column: "columna".into(),
                                        alias: None,
                                    },
                                    Node::ColumnRef {
                                        table: None,
                                        column: "columnb".into(),
                                        alias: None,
                                    },
                                ]),
                                filter: None,
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea join (select * from tableb) on (1 = 1);",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::Inner,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::From {
                                    ty: JoinType::None,
                                    left: Box::new(Node::Select {
                                        projection: vec![Node::All],
                                        from: Box::new(Node::From {
                                                ty: JoinType::None,
                                                left: Box::new(Node::TableRef("tableb".into())),
                                                right: Box::new(Node::None),
                                                using: None,
                                                filter: None,
                                                alias: None,
                                        }),
                                        filter: None,
                                        group: vec![],
                                        order: vec![],
                                        limit: None,
                                    }),
                                    right: Box::new(Node::None),
                                    using: None,
                                    filter: None,
                                    alias: None,
                                }),
                                using: None,
                                filter: Some(Box::new(Node::Expr(
                                    Op::Eq,
                                    vec![
                                        Node::IntegerLiteral(1),
                                        Node::IntegerLiteral(1)
                                    ]
                                ))),
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea join tableb on (tablea.columna = tableb.columna);",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::Inner,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::From {
                                    ty: JoinType::None,
                                    left: Box::new(Node::TableRef("tableb".into())),
                                    right: Box::new(Node::None),
                                    using: None,
                                    filter: None,
                                    alias: None,
                                }),
                                using: None,
                                filter: Some(Box::new(Node::Expr(
                                    Op::Eq,
                                    vec![
                                        Node::ColumnRef {
                                            table: Some("tablea".into()),
                                            column: "columna".into(),
                                            alias: None,
                                        },
                                        Node::ColumnRef {
                                            table: Some("tableb".into()),
                                            column: "columna".into(),
                                            alias: None,
                                        },
                                    ],
                                ))),
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea
                        join tableb on (tablea.columna = tableb.columna)
                        join tablec on (tablea.columna = tablec.columna);",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::Inner,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::From {
                                    ty: JoinType::Inner,
                                    left: Box::new(Node::TableRef("tableb".into())),
                                    right: Box::new(Node::From {
                                        ty: JoinType::None,
                                        left: Box::new(Node::TableRef("tablec".into())),
                                        right: Box::new(Node::None),
                                        using: None,
                                        filter: None,
                                        alias: None,
                                    }),
                                    using: None,
                                    filter: Some(Box::new(Node::Expr(
                                        Op::Eq,
                                        vec![
                                            Node::ColumnRef {
                                                table: Some("tablea".into()),
                                                column: "columna".into(),
                                                alias: None,
                                            },
                                            Node::ColumnRef {
                                                table: Some("tablec".into()),
                                                column: "columna".into(),
                                                alias: None,
                                            },
                                        ],
                                    ))),
                                    alias: None,
                                }),
                                using: None,
                                filter: Some(Box::new(Node::Expr(
                                    Op::Eq,
                                    vec![
                                        Node::ColumnRef {
                                            table: Some("tablea".into()),
                                            column: "columna".into(),
                                            alias: None,
                                        },
                                        Node::ColumnRef {
                                            table: Some("tableb".into()),
                                            column: "columna".into(),
                                            alias: None,
                                        },
                                    ],
                                ))),
                                alias: None,
                            }
                        ),
                        filter: None,
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea where columna is not null;",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(Node::From {
                                ty: JoinType::None,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::None),
                                using: None,
                                filter: None,
                                alias: None,
                        }),
                        filter: Some(Box::new(Node::Expr(
                            Op::Negation,
                            vec![
                                Node::ColumnRef {
                                    table: None,
                                    column: "columna".into(),
                                    alias: None,
                                },
                                Node::Null,
                            ],
                        ))),
                        group: vec![],
                        order: vec![],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea where columna is not null group by columna, columnb order by columnb;",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(Node::From {
                                ty: JoinType::None,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::None),
                                using: None,
                                filter: None,
                                alias: None,
                        }),
                        filter: Some(Box::new(Node::Expr(
                            Op::Negation,
                            vec![
                                Node::ColumnRef {
                                    table: None,
                                    column: "columna".into(),
                                    alias: None,
                                },
                                Node::Null,
                            ],
                        ))),
                        group: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None
                            }
                        ],
                        order: vec![Node::ColumnRef { table: None, column: "columnb".into(), alias: None }],
                        limit: None,
                    }),
                },
                Test {
                    input: "select * from tablea
                        join tableb using (columna)
                        where columna > 1000 and columnb not in (1, 2, 3, 4)
                        group by columna, columnb
                        order by columnb
                        limit 100;",
                    want: Ok(Node::Select {
                        projection: vec![Node::All],
                        from: Box::new(
                            Node::From {
                                ty: JoinType::Inner,
                                left: Box::new(Node::TableRef("tablea".into())),
                                right: Box::new(Node::From {
                                    ty: JoinType::None,
                                    left: Box::new(Node::TableRef("tableb".into())),
                                    right: Box::new(Node::None),
                                    using: None,
                                    filter: None,
                                    alias: None,
                                }),
                                using: Some(vec![
                                    Node::ColumnRef {
                                        table: None,
                                        column: "columna".into(),
                                        alias: None,
                                    },
                                ]),
                                filter: None,
                                alias: None,
                            }
                        ),
                        filter: Some(Box::new(Node::Expr(
                            Op::Conjunction,
                            vec![
                                Node::Expr(
                                    Op::Gt,
                                    vec![
                                        Node::ColumnRef {
                                            table: None,
                                            column: "columna".into(),
                                            alias: None
                                        },
                                        Node::IntegerLiteral(1000)
                                    ]),
                                Node::Expr(
                                    Op::NotIn,
                                    vec![
                                        Node::ColumnRef {
                                            table: None,
                                            column: "columnb".into(),
                                            alias: None
                                        },
                                        Node::In(
                                            vec![
                                                Node::IntegerLiteral(1),
                                                Node::IntegerLiteral(2),
                                                Node::IntegerLiteral(3),
                                                Node::IntegerLiteral(4)
                                            ]
                                        )
                                    ]
                                )
                            ]
                        ))),
                        group: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None
                            }
                        ],
                        order: vec![Node::ColumnRef { table: None, column: "columnb".into(), alias: None }],
                        limit: Some(Box::new(Node::IntegerLiteral(100))),
                    }),
                },
                Test {
                    input: "select * from tablea where columna > 1000 join tableb using (columna);",
                    want: Err(Unexpected(Token::Join)),
                },
                Test {
                    input: "select * from tablea join tableb using (columna) group by columna join tableb using (columnc);",
                    want: Err(Unexpected(Token::Join)),
                },
                Test {
                    input: "select * from tablea join tableb using (columnb) group by columna order by columna join tableb using (columna);",
                    want: Err(Unexpected(Token::Join)),
                },
                Test {
                    input: "select * from tablea join tableb using (columna) order by columna group by columna;",
                    want: Err(Unexpected(Token::Group)),
                },
                Test {
                    input: "select * from tablea join tableb using (columna) limit 100 group by columna;",
                    want: Err(Unexpected(Token::Group)),
                },
                Test {
                    input: "select columna, columnb, from tablea;",
                    want: Err(Unexpected(Token::From)),
                },
                Test {
                    input: "select columna, * from tablea;",
                    want: Err(Unexpected(Token::All)),
                },
                Test {
                    input: "select , columna, columnb from tablea;",
                    want: Err(Unexpected(Token::Comma)),
                },
            ];

        for Test { input, want } in tcs {
            let mut l = Lexer::new(input);
            let have = select(&mut l);

            assert_eq!(want, have, "{}", input);
        }

        Ok(())
    }

    #[test]
    fn test_parse_insert() -> Result<()> {
        let tcs = [
                Test {
                    input: "insert into tablea values (\"test\", 1, \"insert\", 2);",
                    want: Ok(Node::Insert {
                        columns: vec![],
                        table: Box::new(Node::TableRef("tablea".into())),
                        inserts: vec![vec![
                            Node::StringLiteral("test".into()),
                            Node::IntegerLiteral(1),
                            Node::StringLiteral("insert".into()),
                            Node::IntegerLiteral(2),
                        ]],
                    })
                },
                Test {
                    input: "insert into tablea (columna, columnb, columnc, columnd) values (\"test\", 1, \"insert\", 2);",
                    want: Ok(Node::Insert {
                        columns: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnc".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnd".into(),
                                alias: None
                            },
                        ],
                        table: Box::new(Node::TableRef("tablea".into())),
                        inserts: vec![vec![
                            Node::StringLiteral("test".into()),
                            Node::IntegerLiteral(1),
                            Node::StringLiteral("insert".into()),
                            Node::IntegerLiteral(2),
                        ]],
                    })
                },
                Test {
                    input: "insert into tablea (columna, columnb, columnc, columnd)
                        values (\"test\", 1, \"insert\", 2),
                        (\"insert\", 2, \"test\", 1);",
                    want: Ok(Node::Insert {
                        columns: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnc".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnd".into(),
                                alias: None
                            },
                        ],
                        table: Box::new(Node::TableRef("tablea".into())),
                        inserts: vec![
                            vec![
                                Node::StringLiteral("test".into()),
                                Node::IntegerLiteral(1),
                                Node::StringLiteral("insert".into()),
                                Node::IntegerLiteral(2),
                            ],
                            vec![
                                Node::StringLiteral("insert".into()),
                                Node::IntegerLiteral(2),
                                Node::StringLiteral("test".into()),
                                Node::IntegerLiteral(1),
                            ]
                        ],
                    })
                },
                Test {
                    input: "insert tablea (columna, columnb) values (1, NULL)",
                    want: Ok(Node::Insert{
                        columns: vec![
                            Node::ColumnRef {
                                table: None,
                                column: "columna".into(),
                                alias: None
                            },
                            Node::ColumnRef {
                                table: None,
                                column: "columnb".into(),
                                alias: None
                            },
                        ],
                        table: Box::new(Node::TableRef("tablea".into())),
                        inserts: vec![
                            vec![
                                Node::IntegerLiteral(1),
                                Node::Null,
                            ],
                        ],
                    }),
                }
            ];

        for Test { input, want } in tcs {
            let mut l = Lexer::new(input);
            let have = insert(&mut l);

            assert_eq!(want, have);
        }

        Ok(())
    }

    #[test]
    fn test_parse_create() {
        let tcs = [
            Test {
                input: "create table tablea (
                        columna int
                    )",
                want: Ok(Node::Create {
                    table: "tablea".into(),
                    columns: vec![Node::ColumnDef {
                        column: "columna".into(),
                        ty: Type::Int,
                    }],
                }),
            },
            Test {
                input: "create table tablea (
                        columna int,
                        columnb int
                    )",
                want: Ok(Node::Create {
                    table: "tablea".into(),
                    columns: vec![
                        Node::ColumnDef {
                            column: "columna".into(),
                            ty: Type::Int,
                        },
                        Node::ColumnDef {
                            column: "columnb".into(),
                            ty: Type::Int,
                        },
                    ],
                }),
            },
            Test {
                input: "create table tablea (
                        columna int,
                        columnb varchar(255),
                        columnc int
                    )",
                want: Ok(Node::Create {
                    table: "tablea".into(),
                    columns: vec![
                        Node::ColumnDef {
                            column: "columna".into(),
                            ty: Type::Int,
                        },
                        Node::ColumnDef {
                            column: "columnb".into(),
                            ty: Type::Varchar(Box::new(Node::IntegerLiteral(255))),
                        },
                        Node::ColumnDef {
                            column: "columnc".into(),
                            ty: Type::Int,
                        },
                    ],
                }),
            },
        ];

        for Test { input, want } in tcs {
            let mut l = Lexer::new(input);
            let have = create(&mut l);

            assert_eq!(want, have)
        }
    }

    #[test]
    fn test_parse_delete() {
        let tcs = [
            Test {
                input: "delete from tablea",
                want: Ok(Node::Delete {
                    table: "tablea".into(),
                    filter: None,
                    limit: None,
                }),
            },
            Test {
                input: "delete from tablea where 1 = 1",
                want: Ok(Node::Delete {
                    table: "tablea".into(),
                    filter: Some(Box::new(Node::Expr(
                        Op::Eq,
                        vec![Node::IntegerLiteral(1), Node::IntegerLiteral(1)],
                    ))),
                    limit: None,
                }),
            },
            Test {
                input: "delete from tablea where 1 = 1 limit 1000",
                want: Ok(Node::Delete {
                    table: "tablea".into(),
                    filter: Some(Box::new(Node::Expr(
                        Op::Eq,
                        vec![Node::IntegerLiteral(1), Node::IntegerLiteral(1)],
                    ))),
                    limit: Some(Box::new(Node::IntegerLiteral(1000))),
                }),
            },
        ];

        for Test { input, want } in tcs {
            let mut l = Lexer::new(input);
            let have = delete(&mut l);

            assert_eq!(want, have)
        }
    }

    #[test]
    fn test_parse_update() {
        let tcs = [
            Test {
                input: "update tablea set columna = 10",
                want: Ok(Node::Update {
                    table: "tablea".into(),
                    assignments: vec![Node::Assignment {
                        column: "columna".into(),
                        value: Box::new(Node::IntegerLiteral(10)),
                    }],
                    filter: None,
                }),
            },
            Test {
                input: "update tablea set columna = 10, columnb = \"test\" where 1 = 1",
                want: Ok(Node::Update {
                    table: "tablea".into(),
                    assignments: vec![
                        Node::Assignment {
                            column: "columna".into(),
                            value: Box::new(Node::IntegerLiteral(10)),
                        },
                        Node::Assignment {
                            column: "columnb".into(),
                            value: Box::new(Node::StringLiteral("test".into())),
                        },
                    ],
                    filter: Some(Box::new(Node::Expr(
                        Op::Eq,
                        vec![Node::IntegerLiteral(1), Node::IntegerLiteral(1)],
                    ))),
                }),
            },
        ];

        for Test { input, want } in tcs {
            let mut l = Lexer::new(input);
            let have = update(&mut l);

            assert_eq!(want, have)
        }
    }
}
