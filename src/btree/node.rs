use crate::btree::slot::Either;
use crate::catalog::schema::Schema;
use crate::get_ptr;
use crate::page::{DiskObject, PageBuf, PageID, PAGE_SIZE};
use crate::storable::Storable;
use crate::table::tuple::{bytes_to_tuple, Comparand, Data as TupleData};

use super::slot::Slot;

use bytes::BytesMut;
use std::ops::Range;

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
            _ => unreachable!("unexpected NodeType: {value}"),
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

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Internal => write!(f, "Internal"),
            NodeType::Leaf => write!(f, "Leaf"),
        }
    }
}

const NODE_TYPE: usize = 0;
const NODE_IS_ROOT: usize = 1;
const NODE_LEN: Range<usize> = 2..6;
const NODE_NEXT: Range<usize> = 6..10;
const NODE_ID: Range<usize> = 10..14;
const NODE_VALUES_START: usize = 14;

// | NodeType (1) | Root (1) | Len (4) | Max (4) | Next (4) | PageID (4) | Values
#[derive(Clone, Debug, PartialEq)]
pub struct Node<V> {
    pub t: NodeType,
    pub is_root: bool,
    pub next: PageID,
    pub id: PageID,
    values: Vec<Slot<V>>,
}

impl<V> DiskObject for Node<V>
where
    V: Storable,
{
    fn serialise(&self) -> PageBuf {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[NODE_TYPE] = u8::from(self.t);
        ret[NODE_IS_ROOT] = self.is_root as u8;
        ret[NODE_LEN].copy_from_slice(&(self.values.len() as u32).to_be_bytes());
        ret[NODE_NEXT].copy_from_slice(&self.next.to_be_bytes());
        ret[NODE_ID].copy_from_slice(&self.id.to_be_bytes());

        let mut from = NODE_VALUES_START;
        for value in &self.values {
            let size = Either::<V>::SIZE + value.0.size();
            let slot = BytesMut::from(value);
            ret[from..from + size].copy_from_slice(&slot);
            from += size;
        }

        if ret == [0; 4096] {
            panic!("PageBuf::from(Node) produced an empty buffer");
        }

        ret
    }

    fn deserialise(buf: PageBuf, schema: &Schema) -> Self {
        let t = NodeType::from(buf[NODE_TYPE]);
        let is_root = buf[NODE_IS_ROOT] > 0;
        let len = u32::from_be_bytes(buf[NODE_LEN].try_into().unwrap());
        let next = PageID::from_be_bytes(buf[NODE_NEXT].try_into().unwrap());
        let id = PageID::from_be_bytes(buf[NODE_ID].try_into().unwrap());

        let mut values = Vec::new();
        let mut left = &buf[NODE_VALUES_START..];
        for _ in 0..len {
            let tuple = bytes_to_tuple(left, schema);
            let slot_size = tuple.size() + Either::<V>::SIZE;
            let either = Either::from(&left[tuple.size()..slot_size]);
            values.push(Slot(tuple, either));
            left = &left[slot_size..];
        }

        Self { t, is_root, next, id, values }
    }
}

impl<V> IntoIterator for Node<V> {
    type Item = Slot<V>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<V> Node<V>
where
    V: Storable,
{
    pub fn new(id: PageID, t: NodeType, is_root: bool) -> Self {
        Self { t, is_root, next: -1, id, values: Vec::new() }
    }

    /// Split out half of self's values into a new node.
    pub fn split(&mut self, id: PageID) -> Node<V> {
        // All values in the greater half end up in `rest`
        let rest = self.values.split_off(self.values.len() / 2);
        self.is_root = false;

        let mut new = Node { t: self.t, is_root: false, next: -1, id, values: rest };

        if self.t == NodeType::Leaf {
            new.next = self.next;
            self.next = new.id;
        }

        new
    }

    /// Using last values for separators
    pub fn get_separators(
        self,
        other: Option<Node<V>>,
        schema: &Schema,
    ) -> Option<(Slot<V>, Slot<V>)> {
        other.map(|other| (self.get_separator(schema), other.get_separator(schema)))
    }

    /// Using last values for separators
    fn get_separator(self, schema: &Schema) -> Slot<V> {
        let Slot(k, _) = self.values.last().expect("there should be a last slot");
        let k = if self.t == NodeType::Leaf { k.next(schema) } else { k.clone() };
        Slot(k, Either::Pointer(self.id))
    }

    /// Returns `None` if node is a leaf or if no keys were matched and the next key is invalid
    pub fn find_child(&self, key: &TupleData, schema: &Schema) -> Option<PageID> {
        if self.t == NodeType::Leaf {
            return None;
        }

        match self
            .values
            .iter()
            .find(|&s| Comparand(schema, key) < Comparand(schema, &s.0))
            .map(|s| get_ptr!(s))
        {
            None => match self.next {
                -1 => None,
                ptr => Some(ptr),
            },
            ptr => ptr,
        }
    }

    pub fn first_ptr(&self) -> Option<PageID> {
        self.values.first().map(|s| match s.1 {
            Either::Value(_) => unreachable!(),
            Either::Pointer(ptr) => ptr,
        })
    }

    pub fn last_key(&self) -> Option<&TupleData> {
        self.values.last().map(|s| &s.0)
    }

    pub fn almost_full(&self, schema: &Schema) -> bool {
        // TODO: Needs to take into account varchar
        // schema.size() = key, either size = value size + flag
        self.values.len() * (schema.tuple_size() + Either::<V>::SIZE)
            >= (PAGE_SIZE - NODE_VALUES_START) / 4
    }

    pub fn insert(&mut self, slot: Slot<V>, schema: &Schema) -> bool {
        let mut i = self.values.len();
        for (j, Slot(k, _)) in self.values.iter().enumerate() {
            if Comparand(schema, k) == Comparand(schema, &slot.0) {
                // Duplicate key
                return false;
            }

            if Comparand(schema, k) > Comparand(schema, &slot.0) {
                i = j;
                break;
            }
        }

        self.values.insert(i, slot);
        true
    }

    // TODO: unit tests
    // TODO: Accept ref to key and value separately - less cloning
    pub fn replace(&mut self, mut slot: Slot<V>, schema: &Schema) -> Option<Slot<V>> {
        let mut i = self.values.len();
        for (j, Slot(k, _)) in self.values.iter().enumerate() {
            if Comparand(schema, k) == Comparand(schema, &slot.0) {
                std::mem::swap(&mut self.values[j], &mut slot);
                return Some(slot);
            }

            if Comparand(schema, k) > Comparand(schema, &slot.0) {
                i = j;
                break;
            }
        }

        self.values.insert(i, slot);
        None
    }

    pub fn pop_last(&mut self) -> Option<Slot<V>> {
        self.values.pop()
    }

    pub fn first(&self) -> Option<&Slot<V>> {
        self.values.first()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Slot<V>> {
        self.values.iter()
    }

    pub fn get(&self, key: &TupleData, schema: &Schema) -> Option<&Slot<V>> {
        self.values.iter().find(|Slot(k, _)| Comparand(schema, k) == Comparand(schema, key))
    }

    pub fn remove(&mut self, key: &TupleData, schema: &Schema) -> bool {
        if let Some(i) = self
            .values
            .iter()
            .enumerate()
            .find(|(_, Slot(k, _))| Comparand(schema, k) == Comparand(schema, key))
            .map(|(i, _)| i)
        {
            self.values.remove(i);
            true
        } else {
            false
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod test {
    use crate::btree::slot::Either;
    use crate::{column, schema};

    use super::*;

    #[test]
    fn test_serde() {
        let schema = schema! {column!("", Int)};

        let node = Node {
            t: NodeType::Leaf,
            is_root: true,
            next: -1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Value(20)),
                Slot(0.into(), Either::Pointer(1)),
                Slot(20.into(), Either::Value(30)),
                Slot(1.into(), Either::Pointer(2)),
                Slot(30.into(), Either::Value(40)),
                Slot(2.into(), Either::Pointer(3)),
                Slot(40.into(), Either::Value(50)),
                Slot(3.into(), Either::Pointer(4)),
                Slot(50.into(), Either::Value(60)),
                Slot(4.into(), Either::Pointer(5)),
            ],
        };

        let bytes = node.serialise();
        let node2: Node<i32> = Node::deserialise(bytes, &schema);
        assert_eq!(node, node2);
    }

    #[test]
    fn test_split() {
        let mut node = Node {
            t: NodeType::Leaf,
            is_root: true,
            next: -1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Value(1)),
                Slot(20.into(), Either::Value(2)),
                Slot(30.into(), Either::Value(3)),
                Slot(40.into(), Either::Value(4)),
                Slot(50.into(), Either::Value(5)),
                Slot(60.into(), Either::Value(6)),
                Slot(70.into(), Either::Value(7)),
                Slot(80.into(), Either::Value(8)),
                Slot(90.into(), Either::Value(9)),
                Slot(100.into(), Either::Value(10)),
                Slot(110.into(), Either::Value(11)),
            ],
        };

        let new = node.split(1);

        let expected = Node {
            t: NodeType::Leaf,
            is_root: false,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Value(1)),
                Slot(20.into(), Either::Value(2)),
                Slot(30.into(), Either::Value(3)),
                Slot(40.into(), Either::Value(4)),
                Slot(50.into(), Either::Value(5)),
            ],
        };

        assert!(node == expected, "\nExpected: {:?}\n    Node: {:?}\n", expected, node);

        let expected_new = Node {
            t: NodeType::Leaf,
            is_root: false,
            next: -1,
            id: 1,
            values: vec![
                Slot(60.into(), Either::Value(6)),
                Slot(70.into(), Either::Value(7)),
                Slot(80.into(), Either::Value(8)),
                Slot(90.into(), Either::Value(9)),
                Slot(100.into(), Either::Value(10)),
                Slot(110.into(), Either::Value(11)),
            ],
        };

        assert!(new == expected_new, "\nExpected: {:?}\n    Node: {:?}\n", expected_new, new);
    }

    #[test]
    fn test_get_separators_leaf() {
        let schema = schema! {column!("", Int)};

        let node = Node {
            t: NodeType::Leaf,
            is_root: false,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Value(1)),
                Slot(20.into(), Either::Value(2)),
                Slot(30.into(), Either::Value(3)),
                Slot(40.into(), Either::Value(4)),
                Slot(50.into(), Either::Value(5)),
            ],
        };

        let other = Node {
            t: NodeType::Leaf,
            is_root: false,
            next: -1,
            id: 1,
            values: vec![
                Slot(60.into(), Either::Value(6)),
                Slot(70.into(), Either::Value(7)),
                Slot(80.into(), Either::Value(8)),
                Slot(90.into(), Either::Value(9)),
                Slot(100.into(), Either::Value(10)),
                Slot(110.into(), Either::Value(11)),
            ],
        };

        let Some(slots) = node.get_separators(Some(other), &schema) else {
            panic!("expected separators")
        };
        let expected = (Slot(51.into(), Either::Pointer(0)), Slot(111.into(), Either::Pointer(1)));
        assert!(slots == expected);
    }

    #[test]
    fn test_get_separators_internal() {
        let schema = schema! {column!("", Int)};

        let node: Node<i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Pointer(1)),
                Slot(20.into(), Either::Pointer(2)),
                Slot(30.into(), Either::Pointer(3)),
                Slot(40.into(), Either::Pointer(4)),
                Slot(50.into(), Either::Pointer(5)),
            ],
        };

        let other = Node {
            t: NodeType::Internal,
            is_root: false,
            next: -1,
            id: 1,
            values: vec![
                Slot(60.into(), Either::Pointer(6)),
                Slot(70.into(), Either::Pointer(7)),
                Slot(80.into(), Either::Pointer(8)),
                Slot(90.into(), Either::Pointer(9)),
                Slot(100.into(), Either::Pointer(10)),
                Slot(110.into(), Either::Pointer(11)),
            ],
        };

        let Some(slots) = node.get_separators(Some(other), &schema) else {
            panic!("expected separators")
        };
        let expected = (Slot(50.into(), Either::Pointer(0)), Slot(110.into(), Either::Pointer(1)));
        assert!(slots == expected);
    }

    #[test]
    fn test_find_child() {
        let schema = schema! {column!("", Int)};

        let node: Node<i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Pointer(1)),
                Slot(20.into(), Either::Pointer(2)),
                Slot(30.into(), Either::Pointer(3)),
                Slot(40.into(), Either::Pointer(4)),
                Slot(50.into(), Either::Pointer(5)),
            ],
        };

        let a = node.find_child(&25.into(), &schema);
        let b = node.find_child(&30.into(), &schema);
        let c = node.find_child(&60.into(), &schema);

        assert!(a == Some(3));
        assert!(b == Some(4));
        assert!(c == Some(1));
    }

    macro_rules! inserts {
        ($range:expr, $t:ty) => {{
            use rand::{seq::SliceRandom, thread_rng};
            let mut ret = Vec::with_capacity($range.len());

            let mut keys = $range.collect::<Vec<$t>>();
            keys.shuffle(&mut thread_rng());

            for key in keys {
                ret.push(Slot(key.into(), Either::Pointer(0)));
            }

            ret
        }};
    }

    #[test]
    fn test_values() {
        let schema = schema! {column!("", Int)};

        let mut node: Node<i32> =
            Node { t: NodeType::Internal, is_root: false, next: 1, id: 0, values: vec![] };

        // Insert
        let range = -50..50;
        let mut want = inserts!(range, i32);

        for slot in want.iter().rev() {
            node.insert(slot.clone(), &schema);
        }

        want.sort_by(|Slot(k, _), Slot(k0, _)| Comparand(&schema, k).cmp(&Comparand(&schema, k0)));

        assert_eq!(want, node.values);

        // Get
        let mut have = Vec::new();
        for Slot(k, _) in &want {
            match node.get(k, &schema) {
                Some(s) => have.push(s.clone()),
                None => panic!("expected to find {k:?}"),
            }
        }
        assert_eq!(want, have);

        // Delete
        let (first_half, second_half) = want.split_at(want.len() / 2);
        for Slot(k, _) in first_half {
            assert!(node.remove(k, &schema));
        }
        assert_eq!(node.values.len(), second_half.len());

        for Slot(k, _) in first_half {
            if let Some(_) = node.get(k, &schema) {
                panic!("unexpected deleted slot: {k:?}")
            }
        }

        for Slot(k, _) in second_half {
            match node.get(k, &schema) {
                Some(_) => {}
                None => panic!("expected to find {k:?}"),
            }
        }

        // Replace
    }
}
