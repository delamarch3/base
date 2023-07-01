use std::collections::HashMap;

use crate::page::PageID;

/// Maps PageIDs to position within buffer pool
pub type PageTable = HashMap<PageID, usize>;
