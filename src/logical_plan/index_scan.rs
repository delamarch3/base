use crate::catalog::IndexInfo;

use {
    super::{LogicalPlan, LogicalPlanInputs},
    crate::catalog::Schema,
};

pub struct IndexScan {
    index_info: IndexInfo,
}

impl std::fmt::Display for IndexScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let IndexInfo { name, oid, index_ty, .. } = &self.index_info;
        write!(f, "IndexScan: index={} type={} oid={}", name, index_ty, oid)?;

        Ok(())
    }
}

impl LogicalPlan for IndexScan {
    fn schema(&self) -> &Schema {
        &self.index_info.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (None, None)
    }
}

impl IndexScan {
    pub fn new(index_info: IndexInfo) -> Self {
        Self { index_info }
    }
}
