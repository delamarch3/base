pub mod node;
pub mod slot;

use std::marker::PhantomData;

use crate::{
    btree2::{
        node::{Node, NodeType},
        slot::{Either, Slot},
    },
    page::{PageId, PAGE_SIZE},
    page_cache::SharedPageCache,
    storable::Storable,
};

pub enum BTreeError {
    OutOfMemory,
}

pub struct BTree<K, V> {
    root: PageId,
    pc: SharedPageCache,
    max: u32,
    _data: PhantomData<(K, V)>,
}

impl<K, V> BTree<K, V>
where
    K: Storable + Copy + Ord,
    V: Storable + Copy + Eq,
{
    pub fn new(pc: SharedPageCache, max: u32) -> Self {
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
        let root = match self.root {
            -1 => {
                let pin = self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?;
                Node::new(pin.id, self.max, NodeType::Leaf, true)
            }
            id => {
                let pin = self
                    .pc
                    .fetch_page(id)
                    .await
                    .ok_or(BTreeError::OutOfMemory)?;
                let r = pin.read().await;
                Node::from(&r.data[..])
            }
        };
        self.root = root.id;

        if let Some((s, os)) = Self::_insert(&self, root, key, value).await? {
            let new_root_page = self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?;
            let mut root = Node::new(new_root_page.id, self.max, NodeType::Internal, true);
            self.root = root.id;
            root.values.insert(s);
            root.values.insert(os);

            let mut w = new_root_page.write().await;
            w.data = <[u8; PAGE_SIZE]>::from(root);
        }

        Ok(())
    }

    pub async fn _insert(
        &self,
        mut node: Node<K, V>,
        key: K,
        value: V,
    ) -> Result<Option<(Slot<K, V>, Slot<K, V>)>, BTreeError> {
        let mut split = None;
        let mut other = None;
        if node.almost_full() {
            other = Some(self.pc.new_page().await.ok_or(BTreeError::OutOfMemory)?);

            let other_node = node.split(other.unwrap().id);

            if key >= other_node.last_key().expect("there should be a last item") {
                // self is other_node
            }

            split = Some(other_node);
        }

        match node.find_child(key) {
            Some(ptr) => {
                // Follow pageId
            }
            None if node.t == NodeType::Internal => {
                // Create a `next` node
            }
            None => {
                // Reached leaf node
                node.values.replace(Slot(key, Either::Value(value)));
            }
        }

        todo!()
    }
}
