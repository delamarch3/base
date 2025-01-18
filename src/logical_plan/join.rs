use super::{Expr, LogicalPlan};
use crate::catalog::schema::Schema;
use crate::sql::Op;

#[derive(Debug)]
pub enum JoinAlgorithm {
    NestedLoop,
    Hash,
    Merge,
}

impl std::fmt::Display for JoinAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinAlgorithm::NestedLoop => write!(f, "NestedLoopJoin"),
            JoinAlgorithm::Hash => write!(f, "HashJoin"),
            JoinAlgorithm::Merge => write!(f, "MergeJoin"),
        }
    }
}

pub struct Join {
    algo: JoinAlgorithm,
    predicate: Expr,
    pub(super) schema: Schema,
    pub(super) left_input: Box<LogicalPlan>,
    pub(super) right_input: Box<LogicalPlan>,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} [{}]", self.algo, self.predicate)
    }
}

impl From<Join> for LogicalPlan {
    fn from(join: Join) -> Self {
        Self::Join(join)
    }
}

impl Join {
    pub fn new(
        predicate: Expr,
        left_input: impl Into<LogicalPlan>,
        right_input: impl Into<LogicalPlan>,
    ) -> Self {
        let left_input = Box::new(left_input.into());
        let right_input = Box::new(right_input.into());
        let schema = left_input.schema().join(right_input.schema());
        let algo = determine_join_algo(&predicate);
        Self { algo, predicate, schema, left_input, right_input }
    }
}

fn determine_join_algo(expr: &Expr) -> JoinAlgorithm {
    fn is_eq(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryOp { left, op: Op::And, right } => match (left.as_ref(), right.as_ref()) {
                (Expr::Ident(_), Expr::Ident(_))
                | (Expr::Ident(_), Expr::Literal(_))
                | (Expr::Literal(_), Expr::Ident(_))
                | (Expr::Literal(_), Expr::Literal(_)) => false,
                _ => is_eq(left) && is_eq(left),
            },
            Expr::BinaryOp { left, op: Op::Eq, right } => match (left.as_ref(), right.as_ref()) {
                (Expr::Literal(_), Expr::Literal(_)) => false,
                _ => is_eq(left) && is_eq(right),
            },
            Expr::Ident(_) | Expr::Literal(_) => true,
            _ => false,
        }
    }

    // if the predicate consists only of eqs and ands we can use hash join
    if is_eq(expr) {
        JoinAlgorithm::Hash
    } else {
        JoinAlgorithm::NestedLoop
    }
}
