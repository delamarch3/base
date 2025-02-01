use core::panic;

#[derive(PartialEq, Debug)]
pub enum Statement {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Create(Create),
}

#[derive(PartialEq, Debug, Clone)]
pub enum Literal {
    Number(String),
    Decimal(String),
    String(String),
    Bool(bool),
    Null,
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Number(number) => write!(f, "{number}"),
            Literal::Decimal(decimal) => write!(f, "{decimal}"),
            Literal::String(string) => write!(f, "'{string}'"),
            Literal::Bool(bool) => write!(f, "{}", if *bool { "TRUE" } else { "FALSE" }),
            Literal::Null => write!(f, "NULL"),
        }
    }
}

impl From<Literal> for Expr {
    fn from(literal: Literal) -> Self {
        Expr::Literal(literal)
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
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

#[derive(PartialEq, Debug, Clone)]
pub enum Ident {
    Single(String),
    Compound(Vec<String>),
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ident::Single(ident) => write!(f, "{ident}"),
            Ident::Compound(ident) => write_iter(f, &mut ident.iter(), "."),
        }
    }
}

impl std::ops::Index<usize> for Ident {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Ident::Single(ident) => {
                if index > 0 {
                    panic!("index out of bounds: the length is 1 but the index is {index}")
                }

                ident.as_str()
            }
            Ident::Compound(vec) => vec[index].as_str(),
        }
    }
}

impl Ident {
    pub fn qualify(self, with: &str) -> Self {
        match self {
            Ident::Single(ident) => Ident::Compound(vec![with.to_string(), ident]),
            Ident::Compound(mut vec) => {
                vec.insert(0, with.to_string());
                Ident::Compound(vec)
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
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

#[derive(PartialEq, Debug, Clone)]
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
        write_iter(f, &mut args.iter(), ", ")?;
        write!(f, ")")
    }
}

fn write_iter<T: std::fmt::Display, I: Iterator<Item = T>>(
    f: &mut std::fmt::Formatter<'_>,
    iter: &mut I,
    seperator: &'static str,
) -> std::fmt::Result {
    let mut tmp = "";
    for item in iter.by_ref() {
        write!(f, "{tmp}")?;
        tmp = seperator;
        write!(f, "{item}")?;
    }

    Ok(())
}

#[derive(PartialEq, Debug, Clone)]
pub enum Expr {
    Wildcard,
    QualifiedWildcard(Ident),
    Ident(Ident),
    Literal(Literal),
    IsNull { expr: Box<Expr>, negated: bool },
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    Between { expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr> },
    BinaryOp { left: Box<Expr>, op: Op, right: Box<Expr> },
    SubQuery(Box<Query>),
    Function(Function),
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Ident(ident) => write!(f, "{ident}"),
            Expr::Literal(literal) => write!(f, "{literal}"),
            Expr::IsNull { expr, negated } => {
                write!(f, "{expr} IS ")?;
                if *negated {
                    write!(f, "NOT ")?;
                }
                write!(f, "NULL")
            }
            Expr::InList { expr, list, negated: false } => {
                write!(f, "{expr} IN (")?;
                write_iter(f, &mut list.iter(), ", ")?;
                write!(f, ")")
            }
            Expr::InList { expr, list, negated: true } => {
                write!(f, "{expr} NOT IN (")?;
                write_iter(f, &mut list.iter(), ", ")?;
                write!(f, ")")
            }
            Expr::Between { expr, negated: false, low, high } => {
                write!(f, "{expr} BETWEEN {low} AND {high}")
            }
            Expr::Between { expr, negated: true, low, high } => {
                write!(f, "{expr} NOT BETWEEN {low} AND {high}")
            }
            Expr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
            Expr::Function(function) => write!(f, "{function}"),
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard(ident) => write!(f, "{ident}.*"),
            Expr::SubQuery(_) => unimplemented!(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Query {
    pub projection: Vec<SelectItem>,
    pub from: FromTable,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    pub group: Vec<Expr>,
}

#[derive(PartialEq, Debug, Clone)]
pub enum FromTable {
    Table { name: Ident, alias: Option<String> },
    Derived { query: Box<Query>, alias: Option<String> },
    Values { rows: Vec<Vec<Expr>>, alias: Option<String> },
}

#[derive(PartialEq, Debug, Clone)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<Ident>),
}

#[derive(PartialEq, Debug, Clone)]
pub enum JoinType {
    Inner,
    // TODO: add more joins
}

#[derive(PartialEq, Debug, Clone)]
pub struct Join {
    pub from: FromTable,
    pub ty: JoinType,
    pub constraint: JoinConstraint,
}

#[derive(PartialEq, Debug)]
pub struct OrderByExpr {
    pub exprs: Vec<Expr>,
    pub desc: bool,
}

#[derive(PartialEq, Debug, Clone)]
pub enum SelectItem {
    Expr(Expr),
    AliasedExpr { expr: Expr, alias: String },
    QualifiedWildcard(Ident),
    Wildcard,
}

impl std::fmt::Display for SelectItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectItem::Expr(expr) => write!(f, "{expr}"),
            SelectItem::AliasedExpr { expr, alias } => write!(f, "{expr} AS {alias}"),
            SelectItem::QualifiedWildcard(ident) => write!(f, "{ident}.*"),
            SelectItem::Wildcard => write!(f, "*"),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Select {
    pub body: Query,
    pub order: Option<OrderByExpr>,
    pub limit: Option<Expr>,
}

#[derive(PartialEq, Debug)]
pub struct Insert {
    pub table: Ident,
    pub input: InsertInput,
}

#[derive(PartialEq, Debug)]
pub enum InsertInput {
    Values(Vec<Vec<Expr>>),
    Query(Query),
}

#[derive(PartialEq, Debug)]
pub struct Update {
    pub table: Ident,
    pub set: Vec<Assignment>,
    pub filter: Option<Expr>,
}

#[derive(PartialEq, Debug)]
pub struct Assignment {
    pub column: Ident,
    pub expr: Expr,
}

#[derive(PartialEq, Debug)]
pub struct Delete {
    pub table: Ident,
    pub filter: Option<Expr>,
}

#[derive(PartialEq, Debug)]
pub struct Create {
    pub name: Ident,
    pub columns: Vec<ColumnDef>,
}

#[derive(PartialEq, Debug)]
pub enum ColumnType {
    Int,
    Varchar(u16),
}

#[derive(PartialEq, Debug)]
pub struct ColumnDef {
    pub ty: ColumnType,
    pub name: String,
    // TODO: constraints
}
