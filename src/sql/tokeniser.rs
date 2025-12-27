use std::{iter::Peekable, str::Chars};

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Token {
    Eof,

    Keyword(Keyword),
    Ident(String),

    // Literals
    StringLiteral(String),
    NumberLiteral(String),
    DecimalLiteral(String),

    // Operators
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,

    // Symbols
    LParen,
    RParen,
    Semicolon,
    Comma,
    Asterisk,
    Dot,
}

#[derive(Debug, PartialEq, Default, Clone, Copy)]
pub(crate) struct Location {
    line: u64,
    col: u64,
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.col)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum Keyword {
    And,
    As,
    Asc,
    Avg,
    Between,
    By,
    Concat,
    Contains,
    Count,
    Create,
    Delete,
    Desc,
    Distinct,
    Explain,
    False,
    From,
    Group,
    In,
    Insert,
    Int,
    Into,
    Is,
    Join,
    Limit,
    Max,
    Min,
    Not,
    Null,
    On,
    Or,
    Order,
    Select,
    Set,
    Sum,
    Table,
    True,
    Update,
    Using,
    Values,
    Varchar,
    Where,
}

impl TryFrom<String> for Keyword {
    type Error = ();

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let kw = match s.as_str() {
            "AND" => Keyword::And,
            "AS" => Keyword::As,
            "ASC" => Keyword::Asc,
            "AVG" => Keyword::Avg,
            "BETWEEN" => Keyword::Between,
            "BY" => Keyword::By,
            "CONCAT" => Keyword::Concat,
            "CONTAINS" => Keyword::Contains,
            "COUNT" => Keyword::Count,
            "CREATE" => Keyword::Create,
            "DELETE" => Keyword::Delete,
            "DESC" => Keyword::Desc,
            "DISTINCT" => Keyword::Distinct,
            "EXPLAIN" => Keyword::Explain,
            "FALSE" => Keyword::False,
            "FROM" => Keyword::From,
            "GROUP" => Keyword::Group,
            "IN" => Keyword::In,
            "INSERT" => Keyword::Insert,
            "INT" => Keyword::Int,
            "INTO" => Keyword::Into,
            "IS" => Keyword::Is,
            "JOIN" => Keyword::Join,
            "LIMIT" => Keyword::Limit,
            "MAX" => Keyword::Max,
            "MIN" => Keyword::Min,
            "NOT" => Keyword::Not,
            "NULL" => Keyword::Null,
            "ON" => Keyword::On,
            "OR" => Keyword::Or,
            "ORDER" => Keyword::Order,
            "SELECT" => Keyword::Select,
            "SET" => Keyword::Set,
            "SUM" => Keyword::Sum,
            "TABLE" => Keyword::Table,
            "TRUE" => Keyword::True,
            "UPDATE" => Keyword::Update,
            "USING" => Keyword::Using,
            "VALUES" => Keyword::Values,
            "VARCHAR" => Keyword::Varchar,
            "WHERE" => Keyword::Where,

            _ => Err(())?,
        };

        Ok(kw)
    }
}

#[derive(Debug)]
pub enum TokeniserError {
    Unexpected { want: char, have: char, location: Location },
    Unhandled { have: char, location: Location },
}

impl std::fmt::Display for TokeniserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokeniserError::Unexpected { want, have, location } => {
                write!(f, "{location}: unexpected char, want: {want}, have: {have}")
            }
            TokeniserError::Unhandled { location, have } => {
                write!(f, "{location}: unhandled char: {have}")
            }
        }
    }
}

impl std::error::Error for TokeniserError {}

fn unexpected(want: char, have: Option<char>, location: Location) -> TokeniserError {
    TokeniserError::Unexpected { want, have: have.unwrap_or(' '), location }
}

fn unhandled(have: char, location: Location) -> TokeniserError {
    TokeniserError::Unhandled { have, location }
}

pub(crate) struct Tokeniser<'a> {
    chars: Peekable<Chars<'a>>,
    line: u64,
    col: u64,
}

impl<'a> IntoIterator for Tokeniser<'a> {
    type Item = Result<(Token, Location), TokeniserError>;

    type IntoIter = TokeniserIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TokeniserIter { tokeniser: self, eof: false }
    }
}

pub(crate) struct TokeniserIter<'a> {
    tokeniser: Tokeniser<'a>,
    eof: bool,
}

impl<'a> Iterator for TokeniserIter<'a> {
    type Item = Result<(Token, Location), TokeniserError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.eof {
            return None;
        }

        if !self.tokeniser.skip_whitespace() {
            self.eof = true;
            return Some(Ok((Token::Eof, self.tokeniser.location())));
        }

        let location = self.tokeniser.location();
        let result = match self.tokeniser.peek_char() {
            Some(&c) => match c {
                '0'..='9' | '.' => {
                    let mut s = self.tokeniser.peeking_take_while(|c| c.is_numeric());

                    if let Some('.') = self.tokeniser.peek_char() {
                        self.tokeniser.next_char();
                        s.push('.');
                        s.push_str(&self.tokeniser.peeking_take_while(|c| c.is_numeric()));

                        if s == "." {
                            return Some(Ok((Token::Dot, location)));
                        }

                        return Some(Ok((Token::DecimalLiteral(s), location)));
                    }

                    Ok((Token::NumberLiteral(s), location))
                }
                '"' => {
                    self.tokeniser.next_char();
                    let s = self.tokeniser.peeking_take_while(|c| c != '"');
                    match self.tokeniser.next_char() {
                        Some('"') => Ok((Token::StringLiteral(s), location)),
                        have => Err(unexpected('"', have, self.tokeniser.location())),
                    }
                }
                '\'' => {
                    self.tokeniser.next_char();
                    let s = self.tokeniser.peeking_take_while(|c| c != '\'');
                    match self.tokeniser.next_char() {
                        Some('\'') => Ok((Token::StringLiteral(s), location)),
                        have => Err(unexpected('"', have, self.tokeniser.location())),
                    }
                }
                '`' => {
                    self.tokeniser.next_char();
                    let s = self.tokeniser.peeking_take_while(|c| {
                        c.is_alphabetic() || c.is_ascii_digit() || c == '_'
                    });
                    match self.tokeniser.next_char() {
                        Some('`') => Ok((Token::Ident(s), location)),
                        have => Err(unexpected('`', have, self.tokeniser.location())),
                    }
                }
                '>' => {
                    self.tokeniser.next_char();
                    match self.tokeniser.peek_char() {
                        Some('=') => self.tokeniser.consume(Token::Ge, location),
                        _ => Ok((Token::Gt, location)),
                    }
                }
                '<' => {
                    self.tokeniser.next_char();
                    match self.tokeniser.peek_char() {
                        Some('=') => self.tokeniser.consume(Token::Le, location),
                        _ => Ok((Token::Lt, location)),
                    }
                }
                '!' => {
                    self.tokeniser.next_char();
                    match self.tokeniser.next_char() {
                        Some('=') => self.tokeniser.consume(Token::Neq, location),
                        have => Err(unexpected('`', have, location)),
                    }
                }
                '=' => self.tokeniser.consume(Token::Eq, location),
                '(' => self.tokeniser.consume(Token::LParen, location),
                ')' => self.tokeniser.consume(Token::RParen, location),
                ',' => self.tokeniser.consume(Token::Comma, location),
                ';' => self.tokeniser.consume(Token::Semicolon, location),
                '*' => self.tokeniser.consume(Token::Asterisk, location),
                ch if ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_' => {
                    // identifier or keyword:

                    let s = self.tokeniser.peeking_take_while(|c| {
                        c.is_alphabetic() || c.is_ascii_digit() || c == '_'
                    });

                    match Keyword::try_from(s.to_uppercase()) {
                        Ok(kw) => Ok((Token::Keyword(kw), location)),
                        _ => Ok((Token::Ident(s), location)),
                    }
                }
                ch => Err(unhandled(ch, location)),
            },
            None => unreachable!(),
        };

        Some(result)
    }
}

impl<'a> Tokeniser<'a> {
    pub fn new(src: &'a str) -> Self {
        Self { chars: src.chars().peekable(), line: 0, col: 0 }
    }

    pub fn location(&self) -> Location {
        Location { line: self.line, col: self.col }
    }

    fn consume(
        &mut self,
        t: Token,
        location: Location,
    ) -> Result<(Token, Location), TokeniserError> {
        self.next_char();
        Ok((t, location))
    }

    /// Skip any whitespace chars, returns true if there are any remaining chars
    fn skip_whitespace(&mut self) -> bool {
        loop {
            match self.peek_char() {
                Some(c) if c.is_whitespace() => {
                    self.next_char();
                    continue;
                }
                Some('#') => {
                    self.skip_line();
                    continue;
                }
                Some(_) => return true,
                None => return false,
            }
        }
    }

    /// Skip a line, returns true if there are any remaining chars
    fn skip_line(&mut self) -> bool {
        loop {
            match self.peek_char() {
                Some(&c) => {
                    self.next_char();
                    if c == '\n' {
                        break;
                    }
                    continue;
                }
                None => return false,
            }
        }

        self.peek_char().is_some()
    }

    fn peek_char(&mut self) -> Option<&char> {
        self.chars.peek()
    }

    fn next_char(&mut self) -> Option<char> {
        match self.chars.next() {
            Some(c) => {
                if c == '\n' {
                    self.col = 0;
                    self.line += 1;
                } else {
                    self.col += 1;
                }

                Some(c)
            }
            None => None,
        }
    }

    fn peeking_take_while(&mut self, mut predicate: impl FnMut(char) -> bool) -> String {
        let mut s = String::new();
        while let Some(&c) = self.peek_char() {
            if predicate(c) {
                self.next_char();
                s.push(c);
            } else {
                break;
            }
        }

        s
    }
}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! test_tokeniser {
        ($name:tt, $input:expr, $want:expr) => {
            #[test]
            fn $name() {
                let tokeniser = Tokeniser::new($input);
                let have = tokeniser.into_iter().map(|token| token.unwrap().0).collect::<Vec<_>>();
                assert_eq!(Vec::from($want), have);
            }
        };
    }

    macro_rules! test_tokeniser_with_location {
        ($name:tt, $input:expr, $want:expr) => {
            #[test]
            fn $name() {
                let tokeniser = Tokeniser::new($input);
                let have = tokeniser.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
                assert_eq!(Vec::from($want), have);
            }
        };
    }

    test_tokeniser!(test_select, "SELECT", [Token::Keyword(Keyword::Select), Token::Eof]);

    test_tokeniser_with_location!(
        test_select_with_location,
        "SELECT",
        [
            (Token::Keyword(Keyword::Select), Location { line: 0, col: 0 }),
            (Token::Eof, Location { line: 0, col: 6 })
        ]
    );

    test_tokeniser!(
        test_whitespace,
        "    # This is a comment\n\tSELECT #c2\n#This is another comment\nc1",
        [Token::Keyword(Keyword::Select), Token::Ident("c1".into()), Token::Eof]
    );

    test_tokeniser_with_location!(
        test_whitespace_with_location,
        "    # This is a comment\n\tSELECT #c2\n#This is another comment\nc1",
        [
            (Token::Keyword(Keyword::Select), Location { line: 1, col: 1 }),
            (Token::Ident("c1".into()), Location { line: 3, col: 0 }),
            (Token::Eof, Location { line: 3, col: 2 })
        ]
    );

    test_tokeniser!(
        test_select_ident_from,
        "SELECT c1 FROM t1",
        [
            Token::Keyword(Keyword::Select),
            Token::Ident("c1".into()),
            Token::Keyword(Keyword::From),
            Token::Ident("t1".into()),
            Token::Eof
        ]
    );

    test_tokeniser!(
        test_select_multi_ident_from,
        "SELECT s1.t1.c1, c2 FROM s1.t1",
        [
            Token::Keyword(Keyword::Select),
            Token::Ident("s1".into()),
            Token::Dot,
            Token::Ident("t1".into()),
            Token::Dot,
            Token::Ident("c1".into()),
            Token::Comma,
            Token::Ident("c2".into()),
            Token::Keyword(Keyword::From),
            Token::Ident("s1".into()),
            Token::Dot,
            Token::Ident("t1".into()),
            Token::Eof
        ]
    );

    test_tokeniser!(
        test_select_int_and_float,
        "SELECT 1, 2.34, 5., .5",
        [
            Token::Keyword(Keyword::Select),
            Token::NumberLiteral("1".into()),
            Token::Comma,
            Token::DecimalLiteral("2.34".into()),
            Token::Comma,
            Token::DecimalLiteral("5.".into()),
            Token::Comma,
            Token::DecimalLiteral(".5".into()),
            Token::Eof
        ]
    );

    test_tokeniser!(
        test_select_where,
        "SELECT * FROM t1 WHERE a < b OR b <= c OR c > d OR d >= e",
        [
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Ident("t1".into()),
            Token::Keyword(Keyword::Where),
            Token::Ident("a".into()),
            Token::Lt,
            Token::Ident("b".into()),
            Token::Keyword(Keyword::Or),
            Token::Ident("b".into()),
            Token::Le,
            Token::Ident("c".into()),
            Token::Keyword(Keyword::Or),
            Token::Ident("c".into()),
            Token::Gt,
            Token::Ident("d".into()),
            Token::Keyword(Keyword::Or),
            Token::Ident("d".into()),
            Token::Ge,
            Token::Ident("e".into()),
            Token::Eof
        ]
    );

    test_tokeniser!(
        test_select_string,
        "SELECT \"c1\"",
        [Token::Keyword(Keyword::Select), Token::StringLiteral("c1".into()), Token::Eof]
    );

    test_tokeniser!(
        test_select_multi_line_string,
        "SELECT \"c1
2
3\"",
        [Token::Keyword(Keyword::Select), Token::StringLiteral("c1\n2\n3".into()), Token::Eof]
    );

    test_tokeniser!(
        test_select_quoted_ident,
        "SELECT `s1`.`t1`",
        [
            Token::Keyword(Keyword::Select),
            Token::Ident("s1".into()),
            Token::Dot,
            Token::Ident("t1".into()),
            Token::Eof
        ]
    );

    test_tokeniser!(
        test_functions,
        "avg(c1), count(*), min(0)",
        [
            Token::Keyword(Keyword::Avg),
            Token::LParen,
            Token::Ident("c1".into()),
            Token::RParen,
            Token::Comma,
            Token::Keyword(Keyword::Count),
            Token::LParen,
            Token::Asterisk,
            Token::RParen,
            Token::Comma,
            Token::Keyword(Keyword::Min),
            Token::LParen,
            Token::NumberLiteral("0".into()),
            Token::RParen,
            Token::Eof
        ]
    );
}
