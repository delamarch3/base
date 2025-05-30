pub mod node;
pub mod slot;

use std::marker::PhantomData;

use crate::btree::node::{Node, NodeType};
use crate::btree::slot::{Either, Slot};
use crate::catalog::schema::Schema;
use crate::page::{DiskObject, ObjectReadGuard, PageID, PageWriteGuard};
use crate::page_cache::SharedPageCache;
use crate::storable::Storable;
use crate::table::tuple::{Comparand, Data as TupleData};

pub struct BTree<'s, V> {
    root: PageID,
    pc: SharedPageCache,
    schema: &'s Schema,
    _data: PhantomData<V>,
}

impl<'s, V> BTree<'s, V>
where
    V: Storable + Clone + Eq,
{
    pub fn new(pc: SharedPageCache, schema: &'s Schema) -> Self {
        Self { root: -1, pc, schema, _data: PhantomData }
    }

    pub fn new_with_root(pc: SharedPageCache, root: PageID, schema: &'s Schema) -> Self {
        Self { root, pc, schema, _data: PhantomData }
    }

    pub fn root(&self) -> PageID {
        self.root
    }

    // TODO: One thread could split the root whilst another holds a pin to the root. Should double
    // check is_root
    pub fn insert(&mut self, key: &TupleData, value: &V) -> crate::Result<()> {
        let pin;
        let rpage = match self.root {
            -1 => {
                pin = self.pc.new_page()?;
                let node: Node<V> = Node::new(pin.id, NodeType::Leaf, true);
                let mut page = pin.write();
                page.put(&node);
                page
            }
            id => {
                pin = self.pc.fetch_page(id)?;
                pin.write()
            }
        };
        self.root = rpage.id;

        if let Some((s, os)) = self._insert(None, rpage, key, value)? {
            let new_root_page = self.pc.new_page()?;
            let mut new_root = Node::new(new_root_page.id, NodeType::Internal, true);
            self.root = new_root.id;

            new_root.insert(s, self.schema);
            new_root.insert(os, self.schema);

            let mut w = new_root_page.write();
            w.put(&new_root);
        }

        Ok(())
    }

    fn _insert<'a>(
        &'a self,
        mut prev_page: Option<&'a PageWriteGuard<'a>>,
        mut page: PageWriteGuard<'a>,
        key: &TupleData,
        value: &V,
    ) -> crate::Result<Option<(Slot<V>, Slot<V>)>> {
        let mut node: Node<V> = Node::deserialise(page.data, self.schema);

        let mut split = None;
        if node.almost_full(self.schema) {
            let new_page = self.pc.new_page()?;
            let mut npage = new_page.write();
            let mut nnode = node.split(new_page.id);

            if Comparand(self.schema, key) >= Comparand(self.schema, node.last_key().unwrap()) {
                // Write the node
                page.put(&node);

                // We don't need to keep a lock on this side of the tree
                drop(page);

                // Find and insert
                {
                    // Find the child node
                    let ptr = match nnode.find_child(key, self.schema) {
                        Some(ptr) => ptr,
                        None if nnode.t == NodeType::Internal => {
                            // Bump the last node if no pointer found
                            let Slot(_, v) = nnode.pop_last().unwrap();
                            nnode.insert(Slot(key.next(self.schema), v), self.schema);

                            match nnode.find_child(key, self.schema) {
                                Some(ptr) => ptr,
                                None => unreachable!(),
                            }
                        }
                        None => {
                            // Reached leaf node
                            nnode.replace(
                                Slot(key.clone(), Either::Value(value.clone())),
                                self.schema,
                            );
                            npage.put(&nnode);

                            return Ok(node.get_separators(Some(nnode), self.schema));
                        }
                    };

                    let child_page = self.pc.fetch_page(ptr)?;
                    let cpage = child_page.write();

                    prev_page.take();
                    if let Some((s, os)) = self._insert(Some(&npage), cpage, key, value)? {
                        nnode.replace(s, self.schema);
                        nnode.replace(os, self.schema);
                    }

                    // Write the new node
                    npage.put(&nnode);

                    return Ok(node.get_separators(Some(nnode), self.schema));
                }
            }

            // Write the new node
            // Original node is written below
            npage.put(&nnode);

            split = Some(nnode)
        }

        // Find and insert
        {
            // Find the child node
            let ptr = match node.find_child(key, self.schema) {
                Some(ptr) => ptr,
                None if node.t == NodeType::Internal => {
                    // Bump the last node if no pointer found
                    let Slot(_, v) = node.pop_last().unwrap();
                    node.insert(Slot(key.next(self.schema), v), self.schema);

                    match node.find_child(key, self.schema) {
                        Some(ptr) => ptr,
                        None => unreachable!(),
                    }
                }
                None => {
                    // Reached leaf node
                    node.replace(Slot(key.clone(), Either::Value(value.clone())), self.schema);
                    page.put(&node);

                    return Ok(node.get_separators(split, self.schema));
                }
            };

            let child_page = self.pc.fetch_page(ptr)?;
            let cpage = child_page.write();

            prev_page.take();
            if let Some((s, os)) = self._insert(Some(&page), cpage, key, value)? {
                node.replace(s, self.schema);
                node.replace(os, self.schema);
            }

            // Write the original node
            page.put(&node);

            Ok(node.get_separators(split, self.schema))
        }
    }

    pub fn scan(&self) -> crate::Result<Vec<(TupleData, V)>> {
        let mut ret = Vec::new();
        if self.root == -1 {
            return Ok(ret);
        }

        let pin = self.pc.fetch_page(self.root)?;
        let r = pin.read_object::<Node<V>>(self.schema);

        self._scan(None, r, &mut ret)?;

        Ok(ret)
    }

    fn _scan<'a>(
        &'a self,
        mut prev_node: Option<ObjectReadGuard<'a, Node<V>>>,
        node: ObjectReadGuard<'a, Node<V>>,
        acc: &mut Vec<(TupleData, V)>,
    ) -> crate::Result<()> {
        // Find first leaf
        if node.t != NodeType::Leaf {
            let Slot(_, v) = node.first().unwrap();
            match v {
                Either::Pointer(ptr) => {
                    let pin = self.pc.fetch_page(*ptr)?;
                    let r = pin.read_object::<Node<V>>(self.schema);

                    prev_node.take();
                    return self._scan(Some(node), r, acc);
                }
                Either::Value(_) => unreachable!(),
            };
        }

        acc.extend(node.iter().map(|Slot(k, v)| match v {
            Either::Value(v) => (k.clone(), v.clone()),
            Either::Pointer(_) => unreachable!(),
        }));

        if node.next == -1 {
            return Ok(());
        }

        let pin = self.pc.fetch_page(node.next)?;
        let r = pin.read_object::<Node<V>>(self.schema);

        prev_node.take();
        self._scan(Some(node), r, acc)
    }

    pub fn range(&self, from: &TupleData, to: &TupleData) -> crate::Result<Vec<(TupleData, V)>> {
        let mut ret = Vec::new();

        let cur = match self.get_ptr(from, self.root)? {
            Some(c) => c,
            None => return Ok(ret),
        };

        let page = self.pc.fetch_page(cur)?;
        let r = page.read_object::<Node<V>>(self.schema);

        self._range(None, r, &mut ret, from, to)?;

        Ok(ret)
    }

    fn _range<'a>(
        &'a self,
        mut prev_node: Option<ObjectReadGuard<'a, Node<V>>>,
        node: ObjectReadGuard<'a, Node<V>>,
        acc: &mut Vec<(TupleData, V)>,
        from: &TupleData,
        to: &TupleData,
    ) -> crate::Result<()> {
        let next = node.next;
        let len = acc.len();
        acc.extend(
            node.iter()
                .skip_while(|Slot(k, _)| Comparand(self.schema, k) < Comparand(self.schema, from))
                .take_while(|Slot(k, _)| Comparand(self.schema, k) <= Comparand(self.schema, to))
                .cloned()
                .map(|Slot(k, v)| {
                    let v = match v {
                        Either::Value(v) => v,
                        _ => unreachable!(),
                    };
                    (k, v)
                }),
        );
        if len == acc.len() {
            return Ok(());
        }

        if next == -1 {
            return Ok(());
        }

        let next_page = self.pc.fetch_page(next)?;
        let r = next_page.read_object::<Node<V>>(self.schema);

        prev_node.take();

        self._range(Some(node), r, acc, from, to)
    }

    fn get_ptr(&self, key: &TupleData, ptr: PageID) -> crate::Result<Option<PageID>> {
        assert!(ptr != -1);

        let page = self.pc.fetch_page(ptr)?;
        let node = page.read_object::<Node<V>>(self.schema);

        match node.find_child(key, self.schema) {
            Some(ptr) => self.get_ptr(key, ptr),
            None if node.t == NodeType::Leaf => Ok(Some(ptr)),
            None => Ok(None),
        }
    }

    pub fn get(&self, key: &TupleData) -> crate::Result<Option<Slot<V>>> {
        if self.root == -1 {
            return Ok(None);
        }

        self._get(key, self.root)
    }

    fn _get(&self, key: &TupleData, ptr: PageID) -> crate::Result<Option<Slot<V>>> {
        let page = self.pc.fetch_page(ptr)?;
        let node = page.read_object::<Node<V>>(self.schema);

        match node.find_child(key, self.schema) {
            Some(ptr) => self._get(key, ptr),
            None if node.t == NodeType::Leaf => Ok(node.get(key, self.schema).cloned()),
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &TupleData) -> crate::Result<bool> {
        if self.root == -1 {
            return Ok(false);
        }

        self._delete(key, self.root)
    }

    fn _delete(&self, key: &TupleData, ptr: PageID) -> crate::Result<bool> {
        let page = self.pc.fetch_page(ptr)?;
        let mut w = page.write();
        let mut node: Node<V> = Node::deserialise(w.data, self.schema);

        match node.find_child(key, self.schema) {
            Some(ptr) => self._delete(key, ptr),
            None if node.t == NodeType::Leaf => {
                let rem = node.remove(key, self.schema);
                if rem {
                    w.put(&node);
                }
                Ok(rem)
            }
            None => Ok(false),
        }
    }

    #[cfg(test)]
    fn first(&self, ptr: PageID) -> crate::Result<PageID> {
        assert!(ptr != -1);

        let page = self.pc.fetch_page(ptr)?;
        let node = page.read_object::<Node<V>>(self.schema);
        if node.t == NodeType::Leaf {
            return Ok(ptr);
        }

        let Slot(_, v) = node.first().unwrap();
        match v {
            Either::Pointer(ptr) => self.first(*ptr),
            Either::Value(_) => unreachable!(),
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn leaf_count(&self) -> crate::Result<usize> {
        if self.root == -1 {
            return Ok(0);
        }

        let mut ret = 1;
        let mut cur = self.first(self.root)?;

        while cur != -1 {
            let pin = self.pc.fetch_page(cur)?;
            let node = pin.read_object::<Node<V>>(self.schema);

            ret += 1;
            cur = node.next;
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod test {
    use crate::disk::Memory;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::{column, schema};

    use rand::{seq::SliceRandom, thread_rng, Rng};

    use super::*;

    macro_rules! inserts {
        ($range:expr, $t:ty) => {{
            let mut ret = Vec::with_capacity($range.len());

            let mut keys = $range.collect::<Vec<$t>>();
            keys.shuffle(&mut thread_rng());

            for key in keys {
                let value = key + 10;
                ret.push((key.into(), value));
            }

            ret
        }};
    }

    #[test]
    fn test_btree_values() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 16;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let schema = schema! {column!("", Int)};
        let mut btree = BTree::new(pc.clone(), &schema);

        // Insert and get
        let range = -230..230;
        let inserts = inserts!(range, i32);

        for (k, v) in &inserts {
            btree.insert(k, v)?;
        }

        pc.flush_all_pages()?;

        for (k, v) in &inserts {
            let have = btree.get(k)?;
            let want = Some(Slot(k.clone(), Either::Value(*v)));
            assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
        }

        // Delete half and make sure they no longer exist in the tree
        let (first_half, second_half) = inserts.split_at(inserts.len() / 2);
        for (k, _) in first_half {
            btree.delete(k)?;
        }

        pc.flush_all_pages()?;

        for (k, _) in first_half {
            if btree.get(k)?.is_some() {
                panic!("Unexpected deleted key: {:x?}", k.0)
            };
        }

        // Make sure other half can still be accessed
        for (k, v) in second_half {
            let test = match btree.get(k)? {
                Some(t) => t,
                None => panic!("Could not find {:x?}:{v} in the second half", k.0),
            };

            let have = match test.1 {
                Either::Value(v) => v,
                Either::Pointer(_) => unreachable!(),
            };
            assert!(have == *v, "Want: {v}\nHave: {have}");
        }

        // Insert and get a different range
        let range = -25..300;
        let inserts = inserts!(range, i32);

        for (k, v) in &inserts {
            btree.insert(k, v)?;
        }

        pc.flush_all_pages()?;

        for (k, v) in &inserts {
            let have = btree.get(k)?;
            let want = Some(Slot(k.clone(), Either::Value(*v)));
            assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
        }

        Ok(())
    }

    #[test]
    fn test_btree_scan() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 16;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);
        let pc2 = pc.clone();

        let schema = schema! {column!("", Int)};
        let mut btree = BTree::new(pc.clone(), &schema);

        let range = -50..50;
        let mut want = inserts!(range, i32);
        for (k, v) in &want {
            btree.insert(k, v)?;
        }

        pc2.flush_all_pages()?;

        for (k, v) in &want {
            let have = btree.get(k)?;
            let want = Some(Slot(k.clone(), Either::Value(*v)));
            assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
        }

        want.sort_by(|(k, _), (k0, _)| Comparand(&schema, k).cmp(&Comparand(&schema, k0)));
        let have = btree.scan()?;
        assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);

        Ok(())
    }

    /// Test that range() returns the requested range of inserted values in the correct order
    macro_rules! test_btree_range {
        ($name:tt, inserts: $range:expr, from: $from:expr, to: $to:expr) => {
            #[test]
            fn $name() -> crate::Result<()> {
                const MEMORY: usize = PAGE_SIZE * 16;
                const K: usize = 2;
                let disk = Memory::new::<MEMORY>();
                let lru = LRU::new(K);
                let pc = PageCache::new(disk, lru, 0);

                let schema = schema! {column!("", Int)};

                let mut btree = BTree::new(pc.clone(), &schema);

                let mut inserts = inserts!($range, i32);
                for (k, v) in &inserts {
                    btree.insert(k, v)?;
                }

                pc.flush_all_pages()?;

                for (k, v) in &inserts {
                    let have = btree.get(k)?;
                    let want = Some(Slot(k.clone(), Either::Value(*v)));
                    assert_eq!(want, have);
                }

                inserts
                    .sort_by(|(k, _), (k0, _)| Comparand(&schema, k).cmp(&Comparand(&schema, k0)));

                // Avoid creating two ranges for the random range test:
                let from = $from.into();
                let to = $to.into();

                let want = inserts
                    .into_iter()
                    .filter(|(k, _)| {
                        Comparand(&schema, k) >= Comparand(&schema, &from)
                            && Comparand(&schema, k) <= Comparand(&schema, &to)
                    })
                    .collect::<Vec<(TupleData, i32)>>();

                let have = btree.range(&from, &to)?;
                assert_eq!(want, have);

                Ok(())
            }
        };
    }

    test_btree_range! (
        random_range,
        inserts: -50..50,
        from: rand::thread_rng().gen_range(-50..0),
        to: rand::thread_rng().gen_range(0..50)
    );

    test_btree_range! (
        out_of_bounds_range,
        inserts: -50..50,
        from: -100,
        to: -50
    );
}
