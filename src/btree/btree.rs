use std::marker::PhantomData;

use crate::{
    btree::leaf::LeafNode, page::PageId, page_cache::PageCache, storable::Storable,
    table_page::RelationID,
};

use super::{BTreeHeader, BTreeNodeType};

// TODO: proper errors
#[derive(Debug)]
pub enum BTreeError {
    Error,
}
pub type BTreeResult<T> = Result<T, BTreeError>;
use BTreeError::*;

pub struct BTree<K> {
    pc: PageCache,
    root_page_id: PageId,
    _data: PhantomData<K>,

    // Branching factor/order (o). Max number of keys will be o - 1. Must satisfy ⌈o / 2⌉ <= m <= o - 1
    order: u32,
    // Max number of values must satisfy ⌈o / 2⌉ <= n <= o - 1
    leaf_max_len: u32,
}

impl<K> BTree<K>
where
    K: Storable + Ord + Copy,
{
    pub fn new(pm: PageCache, root_page_id: PageId, order: u32, leaf_size: u32) -> Self {
        Self {
            pc: pm,
            root_page_id,
            _data: PhantomData,

            order,
            leaf_max_len: leaf_size,
        }
    }

    pub async fn insert(&self, k: K, rel_id: RelationID) -> BTreeResult<()> {
        let root_page = self.pc.fetch_page(self.root_page_id).await.ok_or(Error)?;
        let mut root_page_w = root_page.write().await;
        let header = BTreeHeader::new(&root_page_w.data);

        if header.is_empty() {
            let mut leaf: LeafNode<K> = LeafNode::new(&root_page_w.data);
            leaf.init(0, self.leaf_max_len);
            leaf.insert(k, rel_id);
            leaf.write_data(&mut root_page_w);

            return Ok(());
        }

        match header.r#type() {
            BTreeNodeType::Invalid => Err(Error)?,
            BTreeNodeType::Internal => {
                // Tree
            }
            BTreeNodeType::Leaf => {
                // List
                if header.almost_full() {
                    // Split
                }

                let mut leaf: LeafNode<K> = LeafNode::new(&root_page_w.data);
                leaf.insert(k, rel_id);
                leaf.write_data(&mut root_page_w);
            }
        }

        todo!()
    }
}
