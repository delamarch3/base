use crate::catalog::IndexInfo;

use super::LogicalPlan;

pub struct IndexScan {
    pub(super) index: IndexInfo,
}

impl std::fmt::Display for IndexScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let IndexInfo { name, oid, index_ty, .. } = &self.index;
        write!(f, "IndexScan {} {} {}", name, index_ty, oid)?;

        Ok(())
    }
}

impl From<IndexScan> for LogicalPlan {
    fn from(index_scan: IndexScan) -> Self {
        Self::IndexScan(index_scan)
    }
}

impl IndexScan {
    pub fn new(index: IndexInfo) -> Self {
        Self { index }
    }
}
