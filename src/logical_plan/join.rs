use {
    super::{Expr, LogicalPlan},
    crate::catalog::schema::Schema,
};

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
        algo: JoinAlgorithm,
        predicate: Expr,
        left_input: impl Into<LogicalPlan>,
        right_input: impl Into<LogicalPlan>,
    ) -> Self {
        let left_input = Box::new(left_input.into());
        let right_input = Box::new(right_input.into());
        let schema = left_input.schema().join(right_input.schema());
        Self { algo, predicate, schema, left_input, right_input }
    }
}
