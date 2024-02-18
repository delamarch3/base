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
    page::{PageBuf, PageId, PageReadGuard, PageWriteGuard},
    page_cache::SharedPageCache,
    storable::Storable,
    writep,
};

use self::slot::Increment;

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

    // TODO: One thread could split the root whilst another holds a pin to the root. Should double
    // check is_root
    pub async fn insert(&mut self, key: K, value: V) -> crate::Result<()> {
        let pin;
        let rpage = match self.root {
            -1 => {
                pin = self.pc.new_page().await?;
                let node: Node<K, V> = Node::new(pin.id, self.max, NodeType::Leaf, true);
                let mut page = pin.write().await;
                writep!(page, &PageBuf::from(&node));
                page
            }
            id => {
                pin = self.pc.fetch_page(id).await?;
                pin.write().await
            }
        };
        self.root = rpage.id;

        if let Some((s, os)) = self._insert(None, rpage, key, value).await? {
            let new_root_page = self.pc.new_page().await?;
            let mut new_root = Node::new(new_root_page.id, self.max, NodeType::Internal, true);
            self.root = new_root.id;

            new_root.values.insert(s);
            new_root.values.insert(os);

            let mut w = new_root_page.write().await;
            writep!(w, &PageBuf::from(&new_root));
        }

        Ok(())
    }

    // TODO: Duplicate code for find and insert
    fn _insert<'a>(
        &'a self,
        mut prev_page: Option<&'a PageWriteGuard<'a>>,
        mut page: PageWriteGuard<'a>,
        key: K,
        value: V,
    ) -> BoxFuture<crate::Result<Option<(Slot<K, V>, Slot<K, V>)>>> {
        async move {
            let mut node = Node::from(&page.data);

            let mut split = None;
            if node.almost_full() {
                let new_page = self.pc.new_page().await?;
                let mut npage = new_page.write().await;
                let mut nnode = node.split(new_page.id);

                if key >= node.last_key().unwrap() {
                    // Write the node
                    writep!(page, &PageBuf::from(&node));

                    // We don't need to keep a lock on this side of the tree
                    drop(page);

                    // Find and insert
                    {
                        // Find the child node
                        let ptr = match nnode.find_child(key) {
                            Some(ptr) => ptr,
                            None if nnode.t == NodeType::Internal => {
                                // Bump the last node if no pointer found
                                let Slot(_, v) = nnode.values.pop_last().unwrap();
                                nnode.values.insert(Slot(key.next(), v));

                                match nnode.find_child(key) {
                                    Some(ptr) => ptr,
                                    None => unreachable!(),
                                }
                            }
                            None => {
                                // Reached leaf node
                                nnode.values.replace(Slot(key, Either::Value(value)));
                                writep!(npage, &PageBuf::from(&nnode));

                                return Ok(node.get_separators(Some(nnode)));
                            }
                        };

                        let child_page = self.pc.fetch_page(ptr).await?;
                        let cpage = child_page.write().await;

                        prev_page.take();
                        if let Some((s, os)) = self._insert(Some(&npage), cpage, key, value).await? {
                            nnode.values.replace(s);
                            nnode.values.replace(os);
                        }

                        // Write the new node
                        writep!(npage, &PageBuf::from(&nnode));

                        return Ok(node.get_separators(Some(nnode)));
                    }
                }

                // Write the new node
                // Original node is written below
                writep!(npage, &PageBuf::from(&nnode));

                split = Some(nnode)
            }

            // Find and insert
            {
                // Find the child node
                let ptr = match node.find_child(key) {
                    Some(ptr) => ptr,
                    None if node.t == NodeType::Internal => {
                        // Bump the last node if no pointer found
                        let Slot(_, v) = node.values.pop_last().unwrap();
                        node.values.insert(Slot(key.next(), v));

                        match node.find_child(key) {
                            Some(ptr) => ptr,
                            None => unreachable!(),
                        }
                    }
                    None => {
                        // Reached leaf node
                        node.values.replace(Slot(key, Either::Value(value)));
                        writep!(page, &PageBuf::from(&node));

                        return Ok(node.get_separators(split));
                    }
                };

                let child_page = self.pc.fetch_page(ptr).await?;
                let cpage = child_page.write().await;

                prev_page.take();
                if let Some((s, os)) = self._insert(Some(&page), cpage, key, value).await? {
                    node.values.replace(s);
                    node.values.replace(os);
                }

                // Write the original node
                writep!(page, &PageBuf::from(&node));

                Ok(node.get_separators(split))
            }
        }
        .boxed()
    }

    pub async fn scan(&self) -> crate::Result<Vec<(K, V)>> {
        let mut ret = Vec::new();
        if self.root == -1 {
            return Ok(ret);
        }

        let pin = self.pc.fetch_page(self.root).await?;
        let r = pin.read().await;

        self._scan(None, r, &mut ret).await?;

        Ok(ret)
    }

    fn _scan<'a>(
        &'a self,
        mut prev_page: Option<PageReadGuard<'a>>,
        page: PageReadGuard<'a>,
        acc: &'a mut Vec<(K, V)>,
    ) -> BoxFuture<crate::Result<()>> {
        async move {
            let node: Node<K, V> = Node::from(&page.data);

            // Find first leaf
            if node.t != NodeType::Leaf {
                let Slot(_, v) = node.values.first().unwrap();
                match v {
                    Either::Pointer(ptr) => {
                        let pin = self.pc.fetch_page(*ptr).await?;
                        let r = pin.read().await;

                        prev_page.take();
                        return self._scan(Some(page), r, acc).await;
                    }
                    Either::Value(_) => unreachable!(),
                };
            }

            acc.extend(node.values.iter().map(|Slot(k, v)| match v {
                Either::Value(v) => (*k, *v),
                Either::Pointer(_) => unreachable!(),
            }));

            if node.next == -1 {
                return Ok(());
            }

            let pin = self.pc.fetch_page(node.next).await?;
            let r = pin.read().await;

            prev_page.take();
            self._scan(Some(page), r, acc).await
        }
        .boxed()
    }

    pub async fn range(&self, from: K, to: K) -> crate::Result<Vec<(K, V)>> {
        let mut ret = Vec::new();

        let cur = match self.get_ptr(from, self.root).await? {
            Some(c) => c,
            None => return Ok(ret),
        };

        let page = self.pc.fetch_page(cur).await?;
        let r = page.read().await;

        self._range(None, r, &mut ret, from, to).await?;

        Ok(ret)
    }

    fn _range<'a>(
        &'a self,
        mut prev_page: Option<PageReadGuard<'a>>,
        page: PageReadGuard<'a>,
        acc: &'a mut Vec<(K, V)>,
        from: K,
        to: K,
    ) -> BoxFuture<crate::Result<()>> {
        async move {
            let node = Node::from(&page.data);

            let len = acc.len();
            acc.extend(
                node.values
                    .into_iter()
                    .skip_while(|&Slot(k, _)| k < from)
                    .take_while(|Slot(k, _)| k <= &to)
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

            if node.next == -1 {
                return Ok(());
            }

            let next_page = self.pc.fetch_page(node.next).await?;
            let r = next_page.read().await;

            prev_page.take();

            self._range(Some(page), r, acc, from, to).await
        }
        .boxed()
    }

    fn get_ptr(&self, key: K, ptr: PageId) -> BoxFuture<crate::Result<Option<PageId>>> {
        async move {
            assert!(ptr != -1);

            let page = self.pc.fetch_page(ptr).await?;
            let r = page.read().await;
            let node: Node<K, V> = Node::from(&r.data);

            match node.find_child(key) {
                Some(ptr) => self.get_ptr(key, ptr).await,
                None if node.t == NodeType::Leaf => Ok(Some(ptr)),
                None => Ok(None),
            }
        }
        .boxed()
    }

    pub async fn get(&self, key: K) -> crate::Result<Option<Slot<K, V>>> {
        if self.root == -1 {
            return Ok(None);
        }

        self._get(key, self.root).await
    }

    fn _get(&self, key: K, ptr: PageId) -> BoxFuture<crate::Result<Option<Slot<K, V>>>> {
        async move {
            let page = self.pc.fetch_page(ptr).await?;
            let r = page.read().await;
            let node = Node::from(&r.data);

            match node.find_child(key) {
                Some(ptr) => self._get(key, ptr).await,
                None if node.t == NodeType::Leaf => {
                    let slot = Slot(key, Either::Pointer(-1));
                    Ok(node.values.get(&slot).copied())
                }
                None => Ok(None),
            }
        }
        .boxed()
    }

    pub async fn delete(&self, key: K) -> crate::Result<bool> {
        if self.root == -1 {
            return Ok(false);
        }

        self._delete(key, self.root).await
    }

    fn _delete(&self, key: K, ptr: PageId) -> BoxFuture<crate::Result<bool>> {
        async move {
            let page = self.pc.fetch_page(ptr).await?;
            let mut w = page.write().await;
            let mut node: Node<K, V> = Node::from(&w.data);

            match node.find_child(key) {
                Some(ptr) => self._delete(key, ptr).await,
                None if node.t == NodeType::Leaf => {
                    let slot = Slot(key, Either::Pointer(-1));
                    let rem = node.values.remove(&slot);
                    if rem {
                        writep!(w, &PageBuf::from(&node));
                    }
                    Ok(rem)
                }
                None => Ok(false),
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

    #[cfg(test)]
    fn first(&self, ptr: PageId) -> BoxFuture<crate::Result<PageId>> {
        async move {
            assert!(ptr != -1);

            let page = self.pc.fetch_page(ptr).await?;
            let r = page.read().await;
            let node: Node<K, V> = Node::from(&r.data);
            if node.t == NodeType::Leaf {
                return Ok(ptr);
            }

            let Slot(_, v) = node.values.first().unwrap();
            match v {
                Either::Pointer(ptr) => self.first(*ptr).await,
                Either::Value(_) => unreachable!(),
            }
        }
        .boxed()
    }

    #[cfg(test)]
    #[allow(dead_code)]
    async fn leaf_count(&self) -> crate::Result<usize> {
        if self.root == -1 {
            return Ok(0);
        }

        let mut ret = 1;
        let mut cur = self.first(self.root).await?;

        while cur != -1 {
            let pin = self.pc.fetch_page(cur).await?;
            let page = pin.read().await;
            let node: Node<K, V> = Node::from(&page.data);

            ret += 1;
            cur = node.next;
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod test {
    use rand::{seq::SliceRandom, thread_rng, Rng};

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
    async fn test_btree() -> crate::Result<()> {
        const MAX: usize = 8;
        const MEMORY: usize = PAGE_SIZE * 128;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRUKHandle::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let mut btree = BTree::new(pc.clone(), MAX as u32);

        // Insert and get
        let range = -50..50;
        let inserts = inserts!(range, i32);

        for (k, v) in &inserts {
            btree.insert(*k, *v).await?;
        }

        pc.flush_all_pages().await?;

        for (k, v) in &inserts {
            let have = btree.get(*k).await?;
            let want = Some(Slot(*k, Either::Value(*v)));
            assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
        }

        // Delete half and make sure they no longer exist in the tree
        let (first_half, second_half) = inserts.split_at(inserts.len() / 2);
        for (k, _) in first_half {
            btree.delete(*k).await?;
        }

        pc.flush_all_pages().await?;

        for (k, _) in first_half {
            match btree.get(*k).await? {
                Some(_) => panic!("Unexpected deleted key: {k}"),
                None => {}
            };
        }

        // Make sure other half can still be accessed
        for (k, v) in second_half {
            let test = match btree.get(*k).await? {
                Some(t) => t,
                None => panic!("Could not find {k}:{v} in the second half"),
            };

            let have = match test.1 {
                Either::Value(v) => v,
                Either::Pointer(_) => unreachable!(),
            };
            assert!(have == *v, "Want: {v}\nHave: {have}");
        }

        // Insert and get a different range
        let range = -25..100;
        let inserts = inserts!(range, i32);

        for (k, v) in &inserts {
            btree.insert(*k, *v).await?;
        }

        pc.flush_all_pages().await?;

        for (k, v) in &inserts {
            let have = btree.get(*k).await?;
            let want = Some(Slot(*k, Either::Value(*v)));
            assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_btree_scan() -> crate::Result<()> {
        const MAX: usize = 8;
        const MEMORY: usize = PAGE_SIZE * 64;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRUKHandle::new(K);
        let pc = PageCache::new(disk, lru, 0);
        let pc2 = pc.clone();

        let mut btree = BTree::new(pc, MAX as u32);

        let range = -50..50;
        let mut want = inserts!(range, i32);
        for (k, v) in &want {
            btree.insert(*k, *v).await?;
        }

        pc2.flush_all_pages().await?;

        for (k, v) in &want {
            let have = btree.get(*k).await?;
            let want = Some(Slot(*k, Either::Value(*v)));
            assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
        }

        want.sort();
        let have = btree.scan().await?;
        assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_btree_range() -> crate::Result<()> {
        struct TestCase {
            name: &'static str,
            range: std::ops::Range<i32>,
            from: i32,
            to: i32,
        }

        const MAX: usize = 8;
        const MEMORY: usize = PAGE_SIZE * 128;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRUKHandle::new(K);
        let pc = PageCache::new(disk, lru, 0);
        let pc2 = pc.clone();

        let tcs = [
            TestCase {
                name: "random range",
                range: -50..50,
                from: rand::thread_rng().gen_range(-50..0),
                to: rand::thread_rng().gen_range(0..50),
            },
            TestCase {
                name: "out of bounds range",
                range: -50..50,
                from: -100,
                to: -50,
            },
        ];

        for TestCase {
            name,
            range,
            from,
            to,
        } in tcs
        {
            let mut btree = BTree::new(pc.clone(), MAX as u32);

            let mut inserts = inserts!(range, i32);
            for (k, v) in &inserts {
                btree.insert(*k, *v).await?;
            }

            pc2.flush_all_pages().await?;

            for (k, v) in &inserts {
                let have = btree.get(*k).await?;
                let want = Some(Slot(*k, Either::Value(*v)));
                assert!(want == have, "\nWant: {:?}\nHave: {:?}\n", want, have);
            }

            inserts.sort();

            let want = inserts
                .into_iter()
                .filter(|s| s.0 >= from && s.0 <= to)
                .collect::<Vec<(i32, i32)>>();

            let have = btree.range(from, to).await?;
            assert!(
                want == have,
                "TestCase \"{}\" failed:\nWant: {:?}\nHave: {:?}\nRange: {:?}",
                name,
                want,
                have,
                (from, to)
            );
        }

        Ok(())
    }
}
