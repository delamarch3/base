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
use futures::{future::BoxFuture, FutureExt};
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
    for<'a> K: Storable + Ord + Copy + Send + 'a,
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
                let split = match Self::insert_list(&self.pc, root_page, k, rel_id).await? {
                    Some(p) => p,
                    None => return Ok(()), // Insert doesn't require split
                };

                let mut split_page_w = split.write().await;
                let mut leaf = LeafNode::<K>::new(&split_page_w.data);

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
                for pair in pairs.iter().skip(pairs.len() / 2) {
                    new_leaf.insert(pair.a, pair.b);
                }

                leaf.next_page_id = new_leaf_page_w.id;

                leaf.write_data(&mut split_page_w);
                new_leaf.write_data(&mut new_leaf_page_w);

                Ok(())
            }
        }
    }

    fn insert_list<'a>(
        pc: &'a PageCache,
        page: Pin<'a>,
        k: K,
        rel_id: RelationID,
    ) -> BoxFuture<'a, BTreeResult<Option<Pin<'a>>>> {
        // If the key is greater than the last element of the page, or there is no nexe page
        // (page_id = 0), insert into the page
        // Else do the same checks on the next page

        async move {
            let mut w = page.write().await;
            let mut leaf = LeafNode::<K>::new(&w.data);

            if leaf.almost_full() {
                // Return page to be split
                drop(w);
                return Ok(Some(page));
            }
            let next_page_id = leaf.next_page_id;
            let last = leaf.pairs().iter().last().unwrap().a;
            if k > last || next_page_id == 0 {
                leaf.insert(k, rel_id);
                leaf.write_data(&mut w);

                return Ok(None);
            }

            let next_page = pc.fetch_page(next_page_id).await.ok_or(Error)?;

            Self::insert_list(pc, next_page, k, rel_id).await
        }
        .boxed()
    }
}
