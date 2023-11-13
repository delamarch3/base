pub mod node;
pub mod slot;

use std::{fmt::Display, marker::PhantomData};

use futures::{future::BoxFuture, FutureExt};

use crate::{
    btree::{
        node::{Node, NodeType},
        slot::{Either, Slot},
    },
    disk::{Disk, FileSystem},
    page::{PageBuf, PageId},
    page_cache::SharedPageCache,
    storable::Storable,
    writep,
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
    K: Storable + Copy + Send + Sync + Display + Ord + Increment,
    V: Storable + Copy + Send + Sync + Display + Eq,
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
                let node = Node::new(pin.id, self.max, NodeType::Leaf, true);
                let mut w = pin.write().await;
                writep!(w, &PageBuf::from(&node));
                node
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
            writep!(w, &PageBuf::from(&new_root));
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

                if key >= node.last_key().unwrap() {
                    // Write the node
                    let page = self
                        .pc
                        .fetch_page(node.id)
                        .await
                        .ok_or(BTreeError::OutOfMemory)?;
                    let mut w = page.write().await;
                    writep!(w, &PageBuf::from(&node));

                    // We don't need to keep a lock on this side of the branch
                    drop(w);

                    // Find the child node
                    let ptr = match self.find_child(&mut new, key).await? {
                        Some(ptr) => ptr,
                        None => {
                            // Reached leaf node
                            new.values.replace(Slot(key, Either::Value(value)));
                            writep!(nw, &PageBuf::from(&new));

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

                    // Dropping because lock will be reacquired in the recursive call. Doubt this is
                    // correct.
                    drop(cw);

                    if let Some((s, os)) = self._insert(next, key, value).await? {
                        new.values.replace(s);
                        new.values.replace(os);
                    }

                    // Write the new node
                    writep!(nw, &PageBuf::from(&new));

                    return Ok(node.get_separators(Some(new)));
                }

                // Write the new node
                // Original node is written below
                writep!(nw, &PageBuf::from(&new));

                split = Some(new)
            }

            let page = self
                .pc
                .fetch_page(node.id)
                .await
                .ok_or(BTreeError::OutOfMemory)?;
            let mut w = page.write().await;

            // Find the child node
            let ptr = match self.find_child(&mut node, key).await? {
                Some(ptr) => ptr,
                None => {
                    // Reached leaf node
                    node.values.replace(Slot(key, Either::Value(value)));
                    writep!(w, &PageBuf::from(&node));

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

            // Dropping because lock will be reacquired in the recursive call. Doubt this is
            // correct.
            drop(cw);

            if let Some((s, os)) = self._insert(next, key, value).await? {
                node.values.replace(s);
                node.values.replace(os);
            }

            // Write the original node
            writep!(w, &PageBuf::from(&node));

            Ok(Node::get_separators(&node, split))
        }
        .boxed()
    }

    async fn find_child(
        &self,
        node: &mut Node<K, V>,
        key: K,
    ) -> Result<Option<PageId>, BTreeError> {
        match node.find_child(key) {
            Some(ptr) => Ok(Some(ptr)),
            None if node.t == NodeType::Internal => {
                // TODO:
                // 1. Determine next node type
                // 2. Create add max() to K's trait
                //    Subsequent calls to this method should return that node
                // 3. If leaf connect last node
                // 4. On split the `os` should use the correct key and replace the correct slot
                // Note: can only be created if the node is root, otherwise unreachable

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
                w.data = PageBuf::from(&new_node);

                let slot = Slot(key.next(), Either::Pointer(new_node.id));
                node.values.insert(slot);

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

    #[cfg(test)]
    #[allow(dead_code)]
    fn print(&self) -> BoxFuture<()> {
        async move {
            if self.root == -1 {
                return;
            }

            self._print(self.root).await;
        }
        .boxed()
    }

    #[cfg(test)]
    fn _print(&self, ptr: PageId) -> BoxFuture<()> {
        async move {
            let page = self.pc.fetch_page(ptr).await.unwrap();
            let r = page.read().await;
            let node: Node<K, V> = Node::from(&r.data);

            dbg!(&node);

            for slot in &node.values {
                match slot.1 {
                    Either::Value(_) => return,
                    Either::Pointer(ptr) => self._print(ptr).await,
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use rand::{seq::SliceRandom, thread_rng};

    use crate::{disk::Memory, page::PAGE_SIZE, page_cache::PageCache, replacer::LRUKHandle};

    use super::*;

    macro_rules! inserts {
        ($range:expr, $t:ty) => {{
            let mut ret = Vec::with_capacity($range.len());

            let mut keys = $range.collect::<Vec<$t>>();
            keys.shuffle(&mut thread_rng());

            for key in keys {
                let value = key + 10;
                ret.push((key, value));
            }

            ret
        }};
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_btree() -> Result<(), BTreeError> {
        const MAX: usize = 8;
        const MEMORY: usize = PAGE_SIZE * 128;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRUKHandle::new(K);
        let pc = PageCache::new(disk, lru, 0);
        let pc2 = pc.clone();

        let mut btree = BTree::new(pc, MAX as u32);

        let slots = 50;
        let inserts = inserts!(-slots..slots, i32);

        for (k, v) in &inserts {
            btree.insert(*k, *v).await?;
        }

        pc2.flush_all_pages().await;

        for (k, v) in inserts {
            let have = btree.get(k).await?;
            let want = Some(Slot(k, Either::Value(v)));
            assert!(have == want, "\nHave: {:?}\nWant: {:?}\n", have, want);
        }

        Ok(())
    }
}
