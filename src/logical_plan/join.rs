use {
    super::{Expr, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub enum JoinAlgorithm {
    NestedLoopJoin,
    HashJoin,
    MergeJoin,
}

impl std::fmt::Display for JoinAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinAlgorithm::NestedLoopJoin => write!(f, "BlockNestedLoopJoin"),
            JoinAlgorithm::HashJoin => write!(f, "HashJoin"),
            JoinAlgorithm::MergeJoin => write!(f, "MergeJoin"),
        }
    }
}

pub struct Join {
    algo: JoinAlgorithm,
    predicate: Expr,
    schema: Schema,
    lhs: Box<dyn LogicalPlan>,
    rhs: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: expr=[{}]", self.algo, self.predicate)
    }
}

impl LogicalPlan for Join {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.lhs), Some(&self.rhs))
    }
}

impl Join {
    pub fn new(
        algo: JoinAlgorithm,
        predicate: Expr,
        lhs: Box<dyn LogicalPlan>,
        rhs: Box<dyn LogicalPlan>,
    ) -> Self {
        let schema = lhs.schema().join(rhs.schema());
        Self { algo, predicate, schema, lhs, rhs }
    }
}
