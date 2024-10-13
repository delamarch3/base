use {
    super::{Expr, LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub enum JoinAlgorithm {
    BlockNestedLoopJoin,
    HashJoin,
    MergeJoin,
}

impl std::fmt::Display for JoinAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinAlgorithm::BlockNestedLoopJoin => write!(f, "BlockNestedLoopJoin"),
            JoinAlgorithm::HashJoin => write!(f, "HashJoin"),
            JoinAlgorithm::MergeJoin => write!(f, "MergeJoin"),
        }
    }
}

pub struct Join {
    join_type: JoinAlgorithm,
    tables: [Expr; 2],
    predicate: Expr,
    schema: Schema,
    lhs: Box<dyn LogicalPlan>,
    rhs: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Join {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: tables={},{} expr={}",
            self.join_type, self.tables[0], self.tables[1], self.predicate
        )
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
        join_type: JoinAlgorithm,
        tables: [Expr; 2],
        predicate: Expr,
        lhs: Box<dyn LogicalPlan>,
        rhs: Box<dyn LogicalPlan>,
    ) -> Self {
        let schema = lhs.schema().extend(rhs.schema());
        Self { join_type, tables, predicate, schema, lhs, rhs }
    }
}
