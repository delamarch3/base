use std::marker::PhantomData;

use crate::{
    btree::{internal::InternalNode, leaf::LeafNode},
    page::{PageID, DEFAULT_PAGE_SIZE},
    page_manager::PageManager,
    storable::Storable,
    table_page::RelationID,
};

pub struct BTree<K, const POOL_SIZE: usize, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    pm: PageManager<POOL_SIZE, PAGE_SIZE>,
    root_page_id: PageID,
    _data: PhantomData<K>,

    internal_max_size: u32,
    leaf_max_size: u32,
}

impl<const POOL_SIZE: usize, const PAGE_SIZE: usize, K> BTree<K, POOL_SIZE, PAGE_SIZE>
where
    K: Storable + Ord + Copy,
{
    pub fn new(
        pm: PageManager<POOL_SIZE, PAGE_SIZE>,
        root_page_id: PageID,
        internal_max_size: u32,
        leaf_max_size: u32,
    ) -> Self {
        Self {
            pm,
            root_page_id,
            _data: PhantomData,

            internal_max_size,
            leaf_max_size,
        }
    }

    pub async fn insert(&self, k: K, rel_id: RelationID) {
        let root_page = match self.pm.fetch_page(self.root_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch btree root page"),
        };
        let root_page_w = root_page.write().await;
        let mut root: InternalNode<K, PAGE_SIZE> = InternalNode::new(&root_page_w.data);

        // First insert - create a new page, insert into internal and leaf nodes:
        if root.len() == 0 {
            root.init(1, self.internal_max_size);

            let new_leaf_page = match self.pm.new_page().await {
                Some(p) => p,
                None => unimplemented!("could not create a new leaf page"),
            };
            let new_leaf_page_w = new_leaf_page.write().await;
            let mut new_leaf: LeafNode<K, PAGE_SIZE> = LeafNode::new(&new_leaf_page_w.data);
            new_leaf.init(1, self.leaf_max_size);

            root.insert(k, new_leaf_page.get_id());
            new_leaf.insert(k, rel_id);

            return;
        }
    }
}
