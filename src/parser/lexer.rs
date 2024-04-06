// TODO: Support functions
#[derive(Debug, PartialEq)]
pub enum Token {
    LParen,
    RParen,

    Create,
    Table,

    Int,
    Varchar,

    Select,
    Insert,
    Update,
    Delete,
    Into,
    Values,
    From,
    Where,
    Join,
    On,
    Using,
    As,
    Limit,
    Conjunction,
    Disjunction,
    Negation,
    Null,
    Semicolon,
    Comma,
    All,
    In,
    Between,
    Is,
    Group,
    By,
    Order,
    Asc,
    Desc,
    Set,

    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,

    StringLiteral(String),
    IntegerLiteral(u64),

    TableAndColumnReference(String, String),
    TableOrColumnReference(String),

    Eof,
}

impl Into<Token> for &str {
    fn into(self) -> Token {
        let lower = self.to_lowercase();

        match &lower[..] {
            "(" => Token::LParen,
            ")" => Token::RParen,

            "create" => Token::Create,
            "table" => Token::Table,
            "int" => Token::Int,
            "varchar" => Token::Varchar,

            "select" => Token::Select,
            "insert" => Token::Insert,
            "update" => Token::Update,
            "delete" => Token::Delete,
            "into" => Token::Into,
            "values" => Token::Values,
            "from" => Token::From,
            "where" => Token::Where,
            "join" => Token::Join,
            "on" => Token::On,
            "using" => Token::Using,
            "as" => Token::As,
            "limit" => Token::Limit,
            "and" => Token::Conjunction,
            "or" => Token::Disjunction,
            "not" => Token::Negation,
            "null" => Token::Null,
            ";" => Token::Semicolon,
            "," => Token::Comma,
            "*" => Token::All,
            "in" => Token::In,
            "between" => Token::Between,
            "is" => Token::Is,
            "group" => Token::Group,
            "by" => Token::By,
            "order" => Token::Order,
            "asc" => Token::Asc,
            "desc" => Token::Desc,
            "set" => Token::Set,

            "=" => Token::Eq,
            "!=" => Token::Neq,
            ">" => Token::Gt,
            ">=" => Token::Ge,
            "<" => Token::Lt,
            "<=" => Token::Le,

            s => {
                // Try integer
                if let Ok(n) = s.parse::<u64>() {
                    return Token::IntegerLiteral(n);
                }

                // Try string
                let mut cs = s.chars();
                if cs.next() == Some('"') && cs.next_back() == Some('"') {
                    return Token::StringLiteral(cs.collect());
                }

                // Try reference
                match s.split_once('.') {
                    Some((table, column)) => {
                        Token::TableAndColumnReference(table.into(), column.into())
                    }
                    None => Token::TableOrColumnReference(s.into()),
                }
            }
        }
    }
}

pub struct Lexer<'a> {
    src: &'a str,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Self { src }
    }

    pub fn next(&mut self) -> Token {
        while !self.src.is_empty() {
            let s = chop(self.src);
            if s.is_empty() {
                self.src = &self.src[1..];
                continue;
            }

            self.src = &self.src[s.len()..];
            return s.into();
        }

        Token::Eof
    }

    pub fn peek(&self) -> Token {
        let mut src = self.src;
        while !src.is_empty() {
            let s = chop(src);
            if s.is_empty() {
                src = &src[1..];
                continue;
            }

            return s.into();
        }

        Token::Eof
    }

    pub fn peek_n(&self, mut n: usize) -> Token {
        let mut t = Token::Eof;
        let mut src = self.src;
        while n > 0 {
            if src.is_empty() {
                return Token::Eof;
            }

            let s = chop(src);
            if s.is_empty() {
                src = &src[1..];
                continue;
            }

            t = s.into();
            src = &src[s.len()..];
            n -= 1;
        }

        t
    }

    pub fn to_vec(mut self) -> Vec<Token> {
        let mut tkns = Vec::new();
        loop {
            let t = self.next();
            let eof = t == Token::Eof;
            tkns.push(t);
            if eof {
                break;
            }
        }

        tkns
    }
}

macro_rules! check_str {
    ($str:ident, $i:expr, [ $($char:expr),* ]) => {
        (
            $(
                &$str[$i..$i+1] == $char ||
            )*
            false
        )
    };

    ($str:ident, $i:expr, $char:expr) => {
        (
            &$str[$i..$i+1] == $char
        )
    };
}

fn chop(src: &str) -> &str {
    let mut i = 0;

    if src.is_empty() {
        return "";
    }

    match &src[0..1] {
        c @ "," => return c,
        c @ "(" => return c,
        c @ ")" => return c,
        c @ ";" => return c,
        _ => {}
    };

    let mut iq = false;
    while i < src.len() {
        if !iq && check_str!(src, i, [" ", "\n", "(", ")", ","]) {
            break;
        }

        let q = check_str!(src, i, "\"");
        if q {
            iq = !iq
        }

        i += 1
    }

    let tmp = &src[0..i];
    if i != 0 && check_str!(tmp, tmp.len() - 1, [",", ")", ";"]) && tmp[0..i].len() > 1 {
        i -= 1
    }

    &src[0..i]
}

#[cfg(test)]
mod test {
    use super::{chop, Lexer, Token};

    #[test]
    fn test_tokenise() {
        struct TestCase {
            input: &'static str,
            want: Vec<Token>,
        }

        let tcs = [
            TestCase {
                input: "",
                want: vec![Token::Eof],
            },
            TestCase {
                input: "select update delete",
                want: vec![Token::Select, Token::Update, Token::Delete, Token::Eof],
            },
            TestCase {
                input: "    select update delete",
                want: vec![Token::Select, Token::Update, Token::Delete, Token::Eof],
            },
            TestCase {
                input: "select update delete    ",
                want: vec![Token::Select, Token::Update, Token::Delete, Token::Eof],
            },
            TestCase {
                input: "select    update    delete",
                want: vec![Token::Select, Token::Update, Token::Delete, Token::Eof],
            },
            TestCase {
                input: "select, update, delete",
                want: vec![
                    Token::Select,
                    Token::Comma,
                    Token::Update,
                    Token::Comma,
                    Token::Delete,
                    Token::Eof
                ],
            },
            TestCase {
                input: "select   ,   update   ,   delete",
                want: vec![
                    Token::Select,
                    Token::Comma,
                    Token::Update,
                    Token::Comma,
                    Token::Delete,
                    Token::Eof
                ],
            },
            TestCase {
                input: ")(",
                want: vec![Token::RParen, Token::LParen, Token::Eof],
            },
            TestCase {
                input: ";,)(",
                want: vec![Token::Semicolon, Token::Comma, Token::RParen, Token::LParen, Token::Eof],
            },
            TestCase {
                input: "\";,)(\n\"\nand",
                want: vec![Token::StringLiteral(";,)(\n".into()), Token::Conjunction,Token::Eof],
            },
            TestCase {
                input: "into(table.columna,table.columnb)values(1,2);",
                want: vec![
                    Token::Into,
                    Token::LParen,
                    Token::TableAndColumnReference("table".into(), "columna".into()),
                    Token::Comma,
                    Token::TableAndColumnReference("table".into(), "columnb".into()),
                    Token::RParen,
                    Token::Values,
                    Token::LParen,
                    Token::IntegerLiteral(1),
                    Token::Comma,
                    Token::IntegerLiteral(2),
                    Token::RParen,
                    Token::Semicolon,
                    Token::Eof
                ],
            },
            TestCase {
                input: "select * from tablea;",
                want: vec![
                    Token::Select,
                    Token::All,
                    Token::From,
                    Token::TableOrColumnReference("tablea".into()),
                    Token::Semicolon,
                    Token::Eof
                ],
            },
            TestCase {
                input: "select tablea.columnA, tablea.columnB from tablea",
                want: vec![
                    Token::Select,
                    Token::TableAndColumnReference("tablea".into(), "columna".into()),
                    Token::Comma,
                    Token::TableAndColumnReference("tablea".into(), "columnb".into()),
                    Token::From,
                    Token::TableOrColumnReference("tablea".into()),
                    Token::Eof
                ],
            },
            TestCase {
                input:
                    "select tablea.columnA, tablea.columnB from tablea where tablea.columnA = \"1234\"
                    and tablea.columnB > 1234",
                want: vec![
                    Token::Select,
                    Token::TableAndColumnReference("tablea".into(), "columna".into()),
                    Token::Comma,
                    Token::TableAndColumnReference("tablea".into(), "columnb".into()),
                    Token::From,
                    Token::TableOrColumnReference("tablea".into()),
                    Token::Where,
                    Token::TableAndColumnReference("tablea".into(), "columna".into()),
                    Token::Eq,
                    Token::StringLiteral("1234".into()),
                    Token::Conjunction,
                    Token::TableAndColumnReference("tablea".into(), "columnb".into()),
                    Token::Gt,
                    Token::IntegerLiteral(1234),
                    Token::Eof
                ],
            },
            TestCase {
                input: "select tablea.columnA, tablea.columnB from tablea
                    join tableB on (tablea.columnA = tableB.columnA)
                    where tablea.columnA = \"1234\" and tablea.columnB > 1234",
                want: vec![
                    Token::Select,
                    Token::TableAndColumnReference("tablea".into(), "columna".into()),
                    Token::Comma,
                    Token::TableAndColumnReference("tablea".into(), "columnb".into()),
                    Token::From,
                    Token::TableOrColumnReference("tablea".into()),
                    Token::Join,
                    Token::TableOrColumnReference("tableb".into()),
                    Token::On,
                    Token::LParen,
                    Token::TableAndColumnReference("tablea".into(), "columna".into()),
                    Token::Eq,
                    Token::TableAndColumnReference("tableb".into(), "columna".into()),
                    Token::RParen,
                    Token::Where,
                    Token::TableAndColumnReference("tablea".into(), "columna".into()),
                    Token::Eq,
                    Token::StringLiteral("1234".into()),
                    Token::Conjunction,
                    Token::TableAndColumnReference("tablea".into(), "columnb".into()),
                    Token::Gt,
                    Token::IntegerLiteral(1234),
                    Token::Eof
                ],
            },
            TestCase {
                input: "insert into tablea (columnA, columnB, columnC, columnD) values (\"a\", 1, \"b\", 2)",
                want: vec![
                    Token::Insert,
                    Token::Into,
                    Token::TableOrColumnReference("tablea".into()),
                    Token::LParen,
                    Token::TableOrColumnReference("columna".into()),
                    Token::Comma,
                    Token::TableOrColumnReference("columnb".into()),
                    Token::Comma,
                    Token::TableOrColumnReference("columnc".into()),
                    Token::Comma,
                    Token::TableOrColumnReference("columnd".into()),
                    Token::RParen,
                    Token::Values,
                    Token::LParen,
                    Token::StringLiteral("a".into()),
                    Token::Comma,
                    Token::IntegerLiteral(1),
                    Token::Comma,
                    Token::StringLiteral("b".into()),
                    Token::Comma,
                    Token::IntegerLiteral(2),
                    Token::RParen,
                    Token::Eof
                ]
            },
            TestCase {
                input: "create table tablea (
                           columnA int,
                           columnB int
                           );",
                want: vec![
                    Token::Create,
                    Token::Table,
                    Token::TableOrColumnReference("tablea".into()),
                    Token::LParen,
                    Token::TableOrColumnReference("columna".into()),
                    Token::Int,
                    Token::Comma,
                    Token::TableOrColumnReference("columnb".into()),
                    Token::Int,
                    Token::RParen,
                    Token::Semicolon,
                    Token::Eof
                ]
            }
        ];

        for TestCase { input, want } in tcs {
            let have = Lexer::new(input).to_vec();
            assert!(want == have, "\nWant: {:?}\nHave: {:?}", want, have);
        }
    }

    #[test]
    fn test_peek_n() {
        struct Test {
            input: &'static str,
            peek: usize,
            want: Token,
        }

        let tcs = [
            Test {
                input: "",
                peek: 2,
                want: Token::Eof,
            },
            Test {
                input: "select",
                peek: 2,
                want: Token::Eof,
            },
            Test {
                input: "select *",
                peek: 2,
                want: Token::All,
            },
            Test {
                input: "select * from tablea where",
                peek: 5,
                want: Token::Where,
            },
        ];

        for Test { input, peek, want } in tcs {
            let have = Lexer::new(input).peek_n(peek);
            assert_eq!(want, have);
        }
    }

    #[test]
    fn test_chop() {
        struct TestCase {
            input: &'static str,
            want: &'static str,
        }

        let tcs = [
            TestCase {
                input: "",
                want: "",
            },
            TestCase {
                input: "select",
                want: "select",
            },
            TestCase {
                input: "select, ",
                want: "select",
            },
            TestCase {
                input: ", update",
                want: ",",
            },
            TestCase {
                input: ",",
                want: ",",
            },
            TestCase {
                input: "(table",
                want: "(",
            },
            TestCase {
                input: "table)",
                want: "table",
            },
        ];

        for TestCase { input, want } in tcs {
            let have = chop(input);
            assert!(want == have, "Want: {:?}, Have: {:?}", want, have);
        }
    }
}
