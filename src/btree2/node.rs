use std::{collections::BTreeSet, ops::Range};

use bytes::BytesMut;

use crate::{
    btree2::slot::Either,
    page::{PageId, PAGE_SIZE},
    storable::Storable,
};

use super::slot::Slot;

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum NodeType {
    Internal,
    Leaf,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            1 => NodeType::Internal,
            2 => NodeType::Leaf,
            _ => unreachable!(),
        }
    }
}

impl From<NodeType> for u8 {
    fn from(value: NodeType) -> Self {
        match value {
            NodeType::Internal => 1,
            NodeType::Leaf => 2,
        }
    }
}

const NODE_TYPE: usize = 0;
const NODE_IS_ROOT: usize = 1;
const NODE_LEN: Range<usize> = 2..6;
const NODE_MAX: Range<usize> = 6..10;
const NODE_NEXT: Range<usize> = 10..14;
const NODE_ID: Range<usize> = 14..18;
const NODE_VALUES_START: usize = 18;

// | NodeType (1) | Root (1) | Len(4) | Max (4) | Next (4) | PageId (4) | Values
#[derive(PartialEq, Clone, Debug)]
pub struct Node<K, V> {
    t: NodeType,
    is_root: bool,
    len: u32,
    max: u32,
    next: PageId,
    id: PageId,
    values: BTreeSet<Slot<K, V>>,
}

impl<K, V> From<&[u8]> for Node<K, V>
where
    K: Storable + Ord,
    V: Storable + Eq,
{
    fn from(value: &[u8]) -> Self {
        let t = NodeType::from(value[NODE_TYPE]);
        let is_root = value[NODE_IS_ROOT] > 0;
        let len = u32::from_be_bytes(value[NODE_LEN].try_into().unwrap());
        let max = u32::from_be_bytes(value[NODE_MAX].try_into().unwrap());
        let next = PageId::from_be_bytes(value[NODE_NEXT].try_into().unwrap());
        let id = PageId::from_be_bytes(value[NODE_ID].try_into().unwrap());

        let mut values = BTreeSet::new();
        let size = Slot::<K, V>::SIZE;

        let left = &value[NODE_VALUES_START..];
        let mut from = 0;
        let mut to = size;
        let mut rem = len;
        while rem > 0 {
            let bytes = &left[from..to];
            let slot = Slot::from(bytes);
            values.insert(slot);
            from += size;
            to += size;
            rem -= 1;
        }

        Self {
            t,
            is_root,
            len,
            max,
            next,
            id,
            values,
        }
    }
}

impl<K, V> From<Node<K, V>> for [u8; PAGE_SIZE]
where
    K: Storable,
    V: Storable,
{
    fn from(node: Node<K, V>) -> Self {
        let mut ret: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

        ret[NODE_TYPE] = u8::from(node.t);
        ret[NODE_IS_ROOT] = node.is_root as u8;
        ret[NODE_LEN].copy_from_slice(&node.len.to_be_bytes());
        ret[NODE_MAX].copy_from_slice(&node.max.to_be_bytes());
        ret[NODE_NEXT].copy_from_slice(&node.next.to_be_bytes());
        ret[NODE_ID].copy_from_slice(&node.id.to_be_bytes());

        let size = Slot::<K, V>::SIZE;
        let mut from = NODE_VALUES_START;
        let mut to = from + size;
        for value in node.values {
            let slot = BytesMut::from(value);
            ret[from..to].copy_from_slice(&slot);
            from += size;
            to += size;
        }

        ret
    }
}

impl<K, V> Node<K, V>
where
    K: Storable + Copy + Ord,
    V: Storable + Copy + Eq,
{
    /// Split out half of self's values into a new node.
    pub fn split(&mut self, id: PageId) -> Node<K, V> {
        let mid = *self
            .values
            .iter()
            .nth(self.values.len() / 2)
            .expect("there should be a mid node");

        // All values in the greater half end up in `rest`
        let rest = self.values.split_off(&mid);
        self.len = self.values.len() as u32;

        let new = Node {
            t: self.t,
            is_root: false,
            len: rest.len() as u32,
            max: self.max,
            next: -1,
            id,
            values: rest,
        };

        if self.t == NodeType::Leaf {
            self.next = new.id;
        }

        new
    }

    pub fn get_separators(&self, other: Option<&Node<K, V>>) -> Option<(Slot<K, V>, Slot<K, V>)>
    where
        K: std::ops::AddAssign<u8>,
    {
        other.map(|other| {
            let k = self.last_key().expect("there should be a last item");
            let mut s = Slot(k, Either::Pointer(self.id));

            let ok = other.last_key().expect("there should be a last item");
            let mut os = Slot(ok, Either::Pointer(other.id));

            if self.t == NodeType::Leaf {
                s.incr_key();
                os.incr_key();
            }

            (s, os)
        })
    }

    pub fn find_child(&self, key: K) -> Option<PageId> {
        if self.t == NodeType::Leaf {
            return None;
        }

        self.values.iter().find(|s| key < s.0).map(|s| match s.1 {
            Either::Value(_) => unreachable!(),
            Either::Pointer(ptr) => ptr,
        })
    }

    #[inline]
    fn last_key(&self) -> Option<K> {
        self.values.last().map(|s| s.0)
    }
}

#[cfg(test)]
mod test {
    use crate::btree2::slot::Either;

    use super::*;

    #[test]
    fn test_from() {
        let node = Node {
            t: NodeType::Leaf,
            is_root: true,
            len: 10,
            max: 20,
            next: -1,
            id: 0,
            values: BTreeSet::from([
                Slot(10, Either::Value(20)),
                Slot(0, Either::Pointer(1)),
                Slot(20, Either::Value(30)),
                Slot(1, Either::Pointer(2)),
                Slot(30, Either::Value(40)),
                Slot(2, Either::Pointer(3)),
                Slot(40, Either::Value(50)),
                Slot(3, Either::Pointer(4)),
                Slot(50, Either::Value(60)),
                Slot(4, Either::Pointer(5)),
            ]),
        };

        let bytes = <[u8; PAGE_SIZE]>::from(node.clone());

        let node2: Node<i32, i32> = Node::from(&bytes[..]);

        assert!(node == node2, "Node: {:?}\n Node2: {:?}", node, node2);
    }

    #[test]
    fn test_split() {
        let mut node = Node {
            t: NodeType::Leaf,
            is_root: true,
            len: 11,
            max: 20,
            next: -1,
            id: 0,
            values: BTreeSet::from([
                Slot(10, Either::Value(1)),
                Slot(20, Either::Value(2)),
                Slot(30, Either::Value(3)),
                Slot(40, Either::Value(4)),
                Slot(50, Either::Value(5)),
                Slot(60, Either::Value(6)),
                Slot(70, Either::Value(7)),
                Slot(80, Either::Value(8)),
                Slot(90, Either::Value(9)),
                Slot(100, Either::Value(10)),
                Slot(110, Either::Value(11)),
            ]),
        };

        let new = node.split(1);

        let expected = Node {
            t: NodeType::Leaf,
            is_root: true,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: BTreeSet::from([
                Slot(10, Either::Value(1)),
                Slot(20, Either::Value(2)),
                Slot(30, Either::Value(3)),
                Slot(40, Either::Value(4)),
                Slot(50, Either::Value(5)),
            ]),
        };

        assert!(
            node == expected,
            "\nExpected: {:?}\n    Node: {:?}\n",
            expected,
            node
        );

        let expected_new = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 6,
            max: 20,
            next: -1,
            id: 1,
            values: BTreeSet::from([
                Slot(60, Either::Value(6)),
                Slot(70, Either::Value(7)),
                Slot(80, Either::Value(8)),
                Slot(90, Either::Value(9)),
                Slot(100, Either::Value(10)),
                Slot(110, Either::Value(11)),
            ]),
        };

        assert!(
            new == expected_new,
            "\nExpected: {:?}\n    Node: {:?}\n",
            expected_new,
            new
        );
    }

    #[test]
    fn test_get_separators_leaf() {
        let node = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: BTreeSet::from([
                Slot(10, Either::Value(1)),
                Slot(20, Either::Value(2)),
                Slot(30, Either::Value(3)),
                Slot(40, Either::Value(4)),
                Slot(50, Either::Value(5)),
            ]),
        };

        let other = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 6,
            max: 20,
            next: -1,
            id: 1,
            values: BTreeSet::from([
                Slot(60, Either::Value(6)),
                Slot(70, Either::Value(7)),
                Slot(80, Either::Value(8)),
                Slot(90, Either::Value(9)),
                Slot(100, Either::Value(10)),
                Slot(110, Either::Value(11)),
            ]),
        };

        let Some(slots) = node.get_separators(Some(&other)) else { panic!() };
        let expected = (Slot(51, Either::Pointer(0)), Slot(111, Either::Pointer(1)));
        assert!(slots == expected);
    }

    #[test]
    fn test_get_separators_internal() {
        // FIXME: K stuck on u8 because of AddAssign constraint
        let node: Node<u8, i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: BTreeSet::from([
                Slot(10, Either::Pointer(1)),
                Slot(20, Either::Pointer(2)),
                Slot(30, Either::Pointer(3)),
                Slot(40, Either::Pointer(4)),
                Slot(50, Either::Pointer(5)),
            ]),
        };

        let other = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 6,
            max: 20,
            next: -1,
            id: 1,
            values: BTreeSet::from([
                Slot(60, Either::Pointer(6)),
                Slot(70, Either::Pointer(7)),
                Slot(80, Either::Pointer(8)),
                Slot(90, Either::Pointer(9)),
                Slot(100, Either::Pointer(10)),
                Slot(110, Either::Pointer(11)),
            ]),
        };

        let Some(slots) = node.get_separators(Some(&other)) else { panic!() };
        let expected = (Slot(50, Either::Pointer(0)), Slot(110, Either::Pointer(1)));
        assert!(slots == expected);
    }

    #[test]
    fn test_find_child() {
        let node: Node<i32, i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: BTreeSet::from([
                Slot(10, Either::Pointer(1)),
                Slot(20, Either::Pointer(2)),
                Slot(30, Either::Pointer(3)),
                Slot(40, Either::Pointer(4)),
                Slot(50, Either::Pointer(5)),
            ]),
        };

        let a = node.find_child(25);
        let b = node.find_child(30);
        let c = node.find_child(60);

        assert!(a == Some(3));
        assert!(b == Some(4));
        assert!(c == None); // TODO: use next field on internal nodes for last slot?
    }
}
