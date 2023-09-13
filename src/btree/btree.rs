use std::marker::PhantomData;

use crate::{
    btree::leaf::LeafNode, page::PageId, page_cache::PageCache, storable::Storable,
    table_page::RelationID,
};

use super::{BTreeHeader, BTreeNodeType};

pub struct BTree<K, const POOL_SIZE: usize> {
    pm: PageCache<POOL_SIZE>,
    root_page_id: PageId,
    _data: PhantomData<K>,

    // Branching factor/order (o). Max number of keys will be o - 1. Must satisfy ⌈o / 2⌉ <= m <= o - 1
    order: u32,
    // Max number of values must satisfy ⌈o / 2⌉ <= n <= o - 1
    leaf_size: u32,
}

impl<const POOL_SIZE: usize, K> BTree<K, POOL_SIZE>
where
    K: Storable + Ord + Copy,
{
    pub fn new(pm: PageCache<POOL_SIZE>, root_page_id: PageId, order: u32, leaf_size: u32) -> Self {
        Self {
            pm,
            root_page_id,
            _data: PhantomData,

            order,
            leaf_size,
        }
    }

    pub async fn insert(&self, k: K, rel_id: RelationID) {
        let root_page = match self.pm.fetch_page(self.root_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch btree root page"),
        };
        let root_page_w = root_page.write().await;
        let header = BTreeHeader::new(&root_page_w.data);
        // let mut root: InternalNode<K, PAGE_SIZE> = InternalNode::new(&root_page_w.data);

        // TODO: First insert, node will be leaf. Will essentially be linked list until correct
        // conditions met.
        if header.len() == 0 {
            // root.init(1, self.leaf_size);

            let new_leaf_page = match self.pm.fetch_page(self.root_page_id).await {
                Some(p) => p,
                None => unimplemented!("could not create a new leaf page"),
            };
            let new_leaf_page_w = new_leaf_page.write().await;
            let mut new_leaf: LeafNode<K> = LeafNode::new(&new_leaf_page_w.data);
            new_leaf.init(1, self.leaf_size);

            new_leaf.insert(k, rel_id);

            return;
        }

        // Traverse linked list
        if header.r#type() == BTreeNodeType::Leaf {
            //
        }
    }
}
