pub mod node;
pub mod slot;

use std::marker::PhantomData;

use futures::{future::BoxFuture, FutureExt};

use crate::{
    btree2::{
        node::{Node, NodeType},
        slot::{Either, Slot},
    },
    disk::{Disk, FileSystem},
    page::{PageId, PAGE_SIZE},
    page_cache::SharedPageCache,
    storable::Storable,
};

use self::slot::Increment;

#[derive(Debug)]
pub enum BTreeError {
    OutOfMemory,
}

pub struct BTree<K, V, D: Disk = FileSystem> {
    root: PageId,
    pc: SharedPageCache<D>,
    max: u32,
    _data: PhantomData<(K, V)>,
}

impl<K, V, D> BTree<K, V, D>
where
    K: Storable + Copy + Send + Sync + Ord + Increment,
    V: Storable + Copy + Send + Sync + Eq,
    D: Disk + Send + Sync,
{
    pub fn new(pc: SharedPageCache<D>, max: u32) -> Self {
        Self {
            root: -1,
            pc,
            max,
            _data: PhantomData,
        }
    }

    // Note: One thread could split the root whilst another holds a pin to the root. Should double
    // check is_root
    pub async fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError> {
        let pin;
        let root = match self.root {
            -1 => {
                pin = self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?;
                Node::new(pin.id, self.max, NodeType::Leaf, true)
            }
            id => {
                pin = self
                    .pc
                    .fetch_page(id)
                    .await
                    .ok_or(BTreeError::OutOfMemory)?;
                let r = pin.read().await;
                Node::from(&r.data)
            }
        };
        self.root = root.id;

        if let Some((s, os)) = Self::_insert(&self, root, key, value).await? {
            let new_root_page = self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?;
            let mut new_root = Node::new(new_root_page.id, self.max, NodeType::Internal, true);
            self.root = new_root.id;

            new_root.values.insert(s);
            new_root.values.insert(os);

            let mut w = new_root_page.write().await;
            w.data = <[u8; PAGE_SIZE]>::from(new_root);
        }

        Ok(())
    }

    fn _insert(
        &self,
        mut node: Node<K, V>,
        key: K,
        value: V,
    ) -> BoxFuture<Result<Option<(Slot<K, V>, Slot<K, V>)>, BTreeError>> {
        async move {
            let mut split = None;
            if node.almost_full() {
                let new_page = self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?;
                let mut nw = new_page.write().await;

                let mut new = node.split(new_page.id);

                if key >= new.last_key().expect("there should be a last item") {
                    // Write the node
                    let page = self
                        .pc
                        .fetch_page(node.id)
                        .await
                        .ok_or(BTreeError::OutOfMemory)?;
                    let mut w = page.write().await;
                    w.data = <[u8; PAGE_SIZE]>::from(&node);

                    // We don't need to keep a lock on this side of the branch
                    drop(w);

                    // Find the child node
                    let ptr = match self.find_child(&new, key).await? {
                        Some(ptr) => ptr,
                        None => {
                            // Reached leaf node
                            new.values.replace(Slot(key, Either::Value(value)));
                            nw.data = <[u8; PAGE_SIZE]>::from(&new);

                            return Ok(node.get_separators(Some(new)));
                        }
                    };

                    // Deserialise child node and recurse
                    let child_page = self
                        .pc
                        .fetch_page(ptr)
                        .await
                        .ok_or(BTreeError::OutOfMemory)?;
                    let cw = child_page.write().await;
                    let next = Node::from(&cw.data);

                    if let Some((s, os)) = self._insert(next, key, value).await? {
                        new.values.insert(s);
                        new.values.insert(os);
                    }

                    // Write the new node
                    nw.data = <[u8; PAGE_SIZE]>::from(&new);

                    return Ok(node.get_separators(Some(new)));
                }

                // Write the new node
                // Original node is written below
                nw.data = <[u8; PAGE_SIZE]>::from(&new);

                split = Some(new)
            }

            let page = self
                .pc
                .fetch_page(node.id)
                .await
                .ok_or(BTreeError::OutOfMemory)?;
            let mut w = page.write().await;

            // Find the child node
            let ptr = match self.find_child(&node, key).await? {
                Some(ptr) => ptr,
                None => {
                    // Reached leaf node
                    node.values.replace(Slot(key, Either::Value(value)));
                    w.data = <[u8; PAGE_SIZE]>::from(&node);

                    return Ok(Node::get_separators(&node, split));
                }
            };

            // Deserialise child node and recurse
            let page = self
                .pc
                .fetch_page(ptr)
                .await
                .ok_or(BTreeError::OutOfMemory)?;
            let cw = page.write().await;
            let next = Node::from(&cw.data);

            if let Some((s, os)) = self._insert(next, key, value).await? {
                node.values.insert(s);
                node.values.insert(os);
            }

            // Write the original node
            w.data = <[u8; PAGE_SIZE]>::from(&node);

            Ok(Node::get_separators(&node, split))
        }
        .boxed()
    }

    async fn find_child(&self, node: &Node<K, V>, key: K) -> Result<Option<PageId>, BTreeError> {
        match node.find_child(key) {
            Some(ptr) => Ok(Some(ptr)),
            None if node.t == NodeType::Internal => {
                let new_node_page = self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?;

                let new_node: Node<K, V> = match node.first_ptr() {
                    Some(ptr) => {
                        let page = self
                            .pc
                            .fetch_page(ptr)
                            .await
                            .ok_or(BTreeError::OutOfMemory)?;
                        let r = page.read().await;
                        let node: Node<K, V> = Node::from(&r.data);

                        match node.t {
                            NodeType::Internal => {
                                Node::new(new_node_page.id, self.max, NodeType::Internal, false)
                            }
                            NodeType::Leaf => {
                                Node::new(new_node_page.id, self.max, NodeType::Leaf, false)
                            }
                        }
                    }
                    None => Node::new(new_node_page.id, self.max, NodeType::Leaf, false),
                };

                let mut w = new_node_page.write().await;
                w.data = <[u8; PAGE_SIZE]>::from(new_node);

                Ok(Some(w.id))
            }
            None => {
                return Ok(None);
            }
        }
    }

    pub async fn get(&self, key: K) -> Result<Option<Slot<K, V>>, BTreeError> {
        if self.root == -1 {
            return Ok(None);
        }

        self._get(key, self.root).await
    }

    fn _get(&self, key: K, ptr: PageId) -> BoxFuture<Result<Option<Slot<K, V>>, BTreeError>> {
        async move {
            let page = self
                .pc
                .fetch_page(ptr)
                .await
                .ok_or(BTreeError::OutOfMemory)?;
            let r = page.read().await;
            let node = Node::from(&r.data);

            match node.find_child(key) {
                Some(ptr) => self._get(key, ptr).await,
                None if node.t == NodeType::Leaf => {
                    let slot = Slot(key, Either::Pointer(0));
                    Ok(node.values.get(&slot).map(|s| *s))
                }
                None => Ok(None),
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::{disk::Memory, page_cache::PageCache, replacer::LRUKHandle};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_btree() -> Result<(), BTreeError> {
        const MAX: usize = 8;
        const MEMORY: usize = PAGE_SIZE * 16;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRUKHandle::new(K);
        let pc = PageCache::new(disk, lru, 0);
        let pc2 = pc.clone();

        let mut btree = BTree::new(pc, MAX as u32);

        btree.insert(8, 8).await?;
        pc2.flush_all_pages().await;

        let have = btree.get(8).await?;
        let want = Some(Slot(8, Either::Value(8)));
        assert!(have == want, "Have: {:?}\nWant: {:?}", have, want);

        Ok(())
    }
}
