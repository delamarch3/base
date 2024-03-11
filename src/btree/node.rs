use std::ops::Range;

use bytes::BytesMut;

use crate::{
    btree::slot::Either,
    catalog::Schema,
    get_ptr,
    page::{PageBuf, PageId, PAGE_SIZE},
    storable::Storable,
    table::tuple::{Comparand, Tuple},
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

const NODE_TYPE: usize = 0;
const NODE_IS_ROOT: usize = 1;
const NODE_LEN: Range<usize> = 2..6;
const NODE_MAX: Range<usize> = 6..10;
const NODE_NEXT: Range<usize> = 10..14;
const NODE_ID: Range<usize> = 14..18;
const NODE_VALUES_START: usize = 18;

// | NodeType (1) | Root (1) | Len (4) | Max (4) | Next (4) | PageId (4) | Values
#[derive(Clone, Debug)]
pub struct Node<'s, V> {
    pub t: NodeType,
    pub is_root: bool,
    len: u32, // TODO: len doesn't need to be in struct
    max: u32,
    pub next: PageId,
    pub id: PageId,
    values: Vec<Slot<V>>,
    schema: &'s Schema,
}

impl<'s, V> PartialEq for Node<'s, V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        #[derive(PartialEq)]
        struct Temp {
            t: NodeType,
            is_root: bool,
            len: u32, // TODO: len doesn't need to be in struct
            max: u32,
            next: PageId,
            id: PageId,
        }

        if (Temp {
            t: self.t,
            is_root: self.is_root,
            len: self.len,
            max: self.max,
            next: self.next,
            id: self.id,
        }) != (Temp {
            t: other.t,
            is_root: other.is_root,
            len: other.len,
            max: other.max,
            next: other.next,
            id: other.id,
        }) {
            return false;
        }

        if self.values.len() != other.values.len() {
            return false;
        }

        for (i, Slot(k, v)) in self.values.iter().enumerate() {
            let Slot(k0, v0) = &other.values[i];

            if Comparand(&self.schema, k) != Comparand(&self.schema, k0) {
                return false;
            }

            if v != v0 {
                return false;
            }
        }

        true
    }
}

impl<'s, V> From<&Node<'s, V>> for PageBuf
where
    V: Storable,
{
    fn from(node: &Node<V>) -> Self {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[NODE_TYPE] = u8::from(node.t);
        ret[NODE_IS_ROOT] = node.is_root as u8;
        ret[NODE_LEN].copy_from_slice(&(node.values.len() as u32).to_be_bytes());
        ret[NODE_MAX].copy_from_slice(&node.max.to_be_bytes());
        ret[NODE_NEXT].copy_from_slice(&node.next.to_be_bytes());
        ret[NODE_ID].copy_from_slice(&node.id.to_be_bytes());

        let mut from = NODE_VALUES_START;
        for value in &node.values {
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
}

impl<'s, V> From<Node<'s, V>> for PageBuf
where
    V: Copy + Storable,
{
    fn from(node: Node<V>) -> Self {
        PageBuf::from(&node)
    }
}

impl<'s, V> Node<'s, V>
where
    V: Storable,
{
    pub fn new(id: PageId, max: u32, t: NodeType, is_root: bool, schema: &'s Schema) -> Self {
        Self {
            t,
            is_root,
            len: 0,
            max,
            next: -1,
            id,
            values: Vec::new(),
            schema,
        }
    }

    pub fn from(buf: &PageBuf, schema: &'s Schema) -> Self {
        let t = NodeType::from(buf[NODE_TYPE]);
        let is_root = buf[NODE_IS_ROOT] > 0;
        let len = u32::from_be_bytes(buf[NODE_LEN].try_into().unwrap());
        let max = u32::from_be_bytes(buf[NODE_MAX].try_into().unwrap());
        let next = PageId::from_be_bytes(buf[NODE_NEXT].try_into().unwrap());
        let id = PageId::from_be_bytes(buf[NODE_ID].try_into().unwrap());

        let mut values = Vec::new();
        let mut left = &buf[NODE_VALUES_START..];
        let mut rem = len;
        while rem > 0 {
            let tuple = Tuple::from(left, schema);
            let slot_size = tuple.size() + Either::<V>::SIZE;
            let either = Either::from(&left[tuple.size()..slot_size]);
            values.push(Slot(tuple, either));
            left = &left[slot_size..];
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
            schema,
        }
    }

    /// Split out half of self's values into a new node.
    pub fn split(&mut self, id: PageId) -> Node<'s, V> {
        // All values in the greater half end up in `rest`
        let rest = self.values.split_off(self.values.len() / 2);
        self.len = self.values.len() as u32;
        self.is_root = false;

        let mut new = Node {
            t: self.t,
            is_root: false,
            len: rest.len() as u32,
            max: self.max,
            next: -1,
            id,
            values: rest,
            schema: self.schema,
        };

        if self.t == NodeType::Leaf {
            new.next = self.next;
            self.next = new.id;
        }

        new
    }

    /// Using last values for separators
    pub fn get_separators(self, other: Option<Node<V>>) -> Option<(Slot<V>, Slot<V>)> {
        other.map(|other| (self.get_separator(), other.get_separator()))
    }

    /// Using last values for separators
    fn get_separator(self) -> Slot<V> {
        let Slot(k, _) = self.values.last().expect("there should be a last slot");
        let k = if self.t == NodeType::Leaf { k.next(self.schema) } else { k.clone() };
        Slot(k, Either::Pointer(self.id))
    }

    /// Returns `None` if node is a leaf or if no keys were matched and the next key is invalid
    pub fn find_child(&self, key: &Tuple) -> Option<PageId> {
        if self.t == NodeType::Leaf {
            return None;
        }

        match self
            .values
            .iter()
            .find(|&s| Comparand(&self.schema, key) < Comparand(&self.schema, &s.0))
            .map(|s| get_ptr!(s))
        {
            None => match self.next {
                -1 => None,
                ptr => Some(ptr),
            },
            ptr => ptr,
        }
    }

    #[inline]
    pub fn first_ptr(&self) -> Option<PageId> {
        self.values.first().map(|s| match s.1 {
            Either::Value(_) => unreachable!(),
            Either::Pointer(ptr) => ptr,
        })
    }

    #[inline]
    pub fn last_key(&self) -> Option<&Tuple> {
        self.values.last().map(|s| &s.0)
    }

    #[inline]
    pub fn almost_full(&self) -> bool {
        self.values.len() >= self.max as usize / 2
    }

    pub fn insert(&mut self, slot: Slot<V>) -> bool {
        let mut i = self.values.len();
        for (j, Slot(k, _)) in self.values.iter().enumerate() {
            if Comparand(&self.schema, k) == Comparand(&self.schema, &slot.0) {
                // Duplicate key
                return false;
            }

            if Comparand(&self.schema, k) > Comparand(&self.schema, &slot.0) {
                i = j;
                break;
            }
        }

        self.values.insert(i, slot);
        true
    }

    // TODO: unit tests
    pub fn replace(&mut self, mut slot: Slot<V>) -> Option<Slot<V>> {
        let mut i = self.values.len();
        for (j, Slot(k, _)) in self.values.iter().enumerate() {
            if Comparand(&self.schema, k) == Comparand(&self.schema, &slot.0) {
                std::mem::swap(&mut self.values[j], &mut slot);
                return Some(slot);
            }

            if Comparand(&self.schema, k) > Comparand(&self.schema, &slot.0) {
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

    pub fn into_iter(self) -> std::vec::IntoIter<Slot<V>> {
        self.values.into_iter()
    }

    pub fn get(&self, key: &Tuple) -> Option<&Slot<V>> {
        self.values
            .iter()
            .find(|Slot(k, _)| Comparand(self.schema, k) == Comparand(self.schema, &key))
    }

    pub fn remove(&mut self, key: &Tuple) -> bool {
        if let Some(i) = self
            .values
            .iter()
            .enumerate()
            .find(|(_, Slot(k, _))| Comparand(self.schema, k) == Comparand(self.schema, &key))
            .map(|(i, _)| i)
        {
            self.values.remove(i);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        btree::slot::Either,
        catalog::{Column, Type},
    };

    use super::*;

    #[test]
    fn test_from() {
        let schema = Schema::new(vec![Column {
            name: "".into(),
            ty: Type::Int,
            offset: 0,
        }]);

        let node = Node {
            t: NodeType::Leaf,
            is_root: true,
            len: 10,
            max: 20,
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
            schema: &schema,
        };

        let bytes = PageBuf::from(node.clone());

        let node2: Node<i32> = Node::from(&bytes, &schema);

        assert_eq!(node, node2);
    }

    #[test]
    fn test_split() {
        let schema = Schema::new(vec![Column {
            name: "".into(),
            ty: Type::Int,
            offset: 0,
        }]);

        let mut node = Node {
            t: NodeType::Leaf,
            is_root: true,
            len: 11,
            max: 20,
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
            schema: &schema,
        };

        let new = node.split(1);

        let expected = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Value(1)),
                Slot(20.into(), Either::Value(2)),
                Slot(30.into(), Either::Value(3)),
                Slot(40.into(), Either::Value(4)),
                Slot(50.into(), Either::Value(5)),
            ],
            schema: &schema,
        };

        assert!(node == expected, "\nExpected: {:?}\n    Node: {:?}\n", expected, node);

        let expected_new = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 6,
            max: 20,
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
            schema: &schema,
        };

        assert!(new == expected_new, "\nExpected: {:?}\n    Node: {:?}\n", expected_new, new);
    }

    #[test]
    fn test_get_separators_leaf() {
        let schema = Schema::new(vec![Column {
            name: "".into(),
            ty: Type::Int,
            offset: 0,
        }]);

        let node = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Value(1)),
                Slot(20.into(), Either::Value(2)),
                Slot(30.into(), Either::Value(3)),
                Slot(40.into(), Either::Value(4)),
                Slot(50.into(), Either::Value(5)),
            ],
            schema: &schema,
        };

        let other = Node {
            t: NodeType::Leaf,
            is_root: false,
            len: 6,
            max: 20,
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
            schema: &schema,
        };

        let Some(slots) = node.get_separators(Some(other)) else {
            panic!("expected separators")
        };
        let expected = (Slot(51.into(), Either::Pointer(0)), Slot(111.into(), Either::Pointer(1)));
        assert!(slots == expected);
    }

    #[test]
    fn test_get_separators_internal() {
        let schema = Schema::new(vec![Column {
            name: "".into(),
            ty: Type::Int,
            offset: 0,
        }]);

        let node: Node<i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Pointer(1)),
                Slot(20.into(), Either::Pointer(2)),
                Slot(30.into(), Either::Pointer(3)),
                Slot(40.into(), Either::Pointer(4)),
                Slot(50.into(), Either::Pointer(5)),
            ],
            schema: &schema,
        };

        let other = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 6,
            max: 20,
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
            schema: &schema,
        };

        let Some(slots) = node.get_separators(Some(other)) else {
            panic!("expected separators")
        };
        let expected = (Slot(50.into(), Either::Pointer(0)), Slot(110.into(), Either::Pointer(1)));
        assert!(slots == expected);
    }

    #[test]
    fn test_find_child() {
        let schema = Schema::new(vec![Column {
            name: "".into(),
            ty: Type::Int,
            offset: 0,
        }]);

        let node: Node<i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 5,
            max: 20,
            next: 1,
            id: 0,
            values: vec![
                Slot(10.into(), Either::Pointer(1)),
                Slot(20.into(), Either::Pointer(2)),
                Slot(30.into(), Either::Pointer(3)),
                Slot(40.into(), Either::Pointer(4)),
                Slot(50.into(), Either::Pointer(5)),
            ],
            schema: &schema,
        };

        let a = node.find_child(&25.into());
        let b = node.find_child(&30.into());
        let c = node.find_child(&60.into());

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
        let schema = Schema::new(vec![Column {
            name: "".into(),
            ty: Type::Int,
            offset: 0,
        }]);

        let mut node: Node<i32> = Node {
            t: NodeType::Internal,
            is_root: false,
            len: 0,
            max: 0,
            next: 1,
            id: 0,
            values: vec![],
            schema: &schema,
        };

        // Insert
        let range = -50..50;
        let mut want = inserts!(range, i32);

        for slot in want.iter().rev() {
            node.insert(slot.clone());
        }

        want.sort_by(|Slot(k, _), Slot(k0, _)| Comparand(&schema, k).cmp(&Comparand(&schema, k0)));

        assert_eq!(want, node.values);

        // Get
        let mut have = Vec::new();
        for Slot(k, _) in &want {
            match node.get(&k) {
                Some(s) => have.push(s.clone()),
                None => panic!("expected to find {k:?}"),
            }
        }
        assert_eq!(want, have);

        // Delete
        let (first_half, second_half) = want.split_at(want.len() / 2);
        for Slot(k, _) in first_half {
            assert!(node.remove(&k));
        }
        assert_eq!(node.values.len(), second_half.len());

        for Slot(k, _) in first_half {
            match node.get(&k) {
                Some(_) => panic!("unexpected deleted slot: {k:?}"),
                None => {}
            }
        }

        for Slot(k, _) in second_half {
            match node.get(&k) {
                Some(_) => {}
                None => panic!("expected to find {k:?}"),
            }
        }

        // Replace
    }
}
