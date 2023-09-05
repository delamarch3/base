use std::marker::PhantomData;

use bytes::BytesMut;

use crate::{
    btree::{internal::InternalNode, leaf::LeafNode},
    page::{PageID, DEFAULT_PAGE_SIZE},
    page_manager::PageManager,
    pair::PairType,
    table_page::RelationID,
};

pub struct BTree<K, const POOL_SIZE: usize, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    pm: PageManager<POOL_SIZE, PAGE_SIZE>,
    header_page_id: PageID,
    _data: PhantomData<K>,
}

impl<const POOL_SIZE: usize, const PAGE_SIZE: usize, K> BTree<K, POOL_SIZE, PAGE_SIZE>
where
    for<'a> PairType<K>: Into<BytesMut> + From<&'a [u8]> + Copy + Ord,
    K: Ord,
{
    pub fn new(header_page_id: PageID, pm: PageManager<POOL_SIZE, PAGE_SIZE>) -> Self {
        Self {
            pm,
            header_page_id,
            _data: PhantomData,
        }
    }

    pub async fn insert(&self, k: K, rel_id: RelationID) {
        let header_page = match self.pm.fetch_page(self.header_page_id).await {
            Some(p) => p,
            None => unimplemented!("could not fetch btree header page"),
        };
        let header_page_w = header_page.write().await;
        let header: InternalNode<K, PAGE_SIZE> = InternalNode::new(&header_page_w.data);

        // First item - create a new page, insert into internal and leaf nodes:
        if header.len() == 0 {
            let new_leaf_page = match self.pm.new_page().await {
                Some(p) => p,
                None => unimplemented!("could not create a new leaf page"),
            };
            let new_leaf_page_w = new_leaf_page.write().await;
            let mut new_leaf: LeafNode<K, PAGE_SIZE> = LeafNode::new(&new_leaf_page_w.data);

            new_leaf.insert(k, rel_id);
        }
    }
}
