use std::marker::PhantomData;

use crate::{
    btree::leaf::LeafNode,
    page::PageId,
    page_cache::{PageCache, Pin},
    storable::Storable,
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
    for<'async_recursion> K: Storable + Ord + Copy + Send + 'async_recursion,
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
            let mut leaf = LeafNode::<K>::new(&root_page_w.data);
            leaf.init(0, self.leaf_max_len);
            leaf.insert(k, rel_id);
            leaf.write_data(&mut root_page_w);

            return Ok(());
        }

        drop(root_page_w);
        match header.r#type() {
            BTreeNodeType::Invalid => Err(Error)?,
            BTreeNodeType::Internal => {
                // Tree
                todo!()
            }
            BTreeNodeType::Leaf => {
                if !header.almost_full() {
                    return Self::insert_list(&self.pc, root_page, k, rel_id).await;
                }

                let mut root_page_w = root_page.write().await;
                let mut leaf: LeafNode<K> = LeafNode::new(&root_page_w.data);

                // Split
                let mut pairs = leaf.clear();
                pairs.push((k, rel_id).into());

                let new_leaf_page = self.pc.new_page().await.ok_or(Error)?;
                let mut new_leaf_page_w = new_leaf_page.write().await;
                let mut new_leaf = LeafNode::<K>::new(&new_leaf_page_w.data);
                leaf.init(0, self.leaf_max_len);

                // Since we're using `BinaryHeap` the first iteration will store the greater
                // elements in the first page
                for pair in pairs.iter().take(pairs.len() / 2) {
                    leaf.insert(pair.a, pair.b);
                }
                for pair in pairs.iter().skip(pairs.len() / 2).take(pairs.len() / 2) {
                    new_leaf.insert(pair.a, pair.b);
                }

                leaf.next_page_id = new_leaf_page_w.id;

                leaf.write_data(&mut root_page_w);
                new_leaf.write_data(&mut new_leaf_page_w);

                Ok(())
            }
        }
    }

    #[async_recursion::async_recursion]
    async fn insert_list<'a: 'async_recursion>(
        pc: &PageCache,
        first: Pin<'a>,
        k: K,
        rel_id: RelationID,
    ) -> BTreeResult<()> {
        // Check if the key is greater than the last
        // If the key is greater than the last element of the page, or there is no next
        // page (page_id = 0), insert into the page
        // Else do the same checks on the next page

        let mut first_page_w = first.write().await;
        let mut first = LeafNode::<K>::new(&first_page_w.data);

        let cur = &mut first;
        let next_page_id = cur.next_page_id;
        let last = cur.pairs().iter().last().unwrap().a;
        if k > last || next_page_id == 0 {
            cur.insert(k, rel_id);
            cur.write_data(&mut first_page_w);

            return Ok(());
        }

        let next_page = pc.fetch_page(next_page_id).await.ok_or(Error)?;

        Self::insert_list(pc, next_page, k, rel_id).await
    }
}
