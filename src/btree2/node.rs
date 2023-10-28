use std::{collections::BTreeSet, ops::Range};

use bytes::BytesMut;

use crate::{
    page::{PageId, PAGE_SIZE},
    storable::Storable,
};

use super::slot::Slot;

#[derive(PartialEq, Clone, Debug)]
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
const NODE_VALUES_START: usize = 14;

// | NodeType (1) | Root (1) | Len(4) | Max (4) | Next (4) | Values
#[derive(PartialEq, Clone, Debug)]
pub struct Node<K, V> {
    t: NodeType,
    is_root: bool,
    len: u32,
    max: u32,
    next: PageId,
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
        let next = i32::from_be_bytes(value[NODE_NEXT].try_into().unwrap());

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

#[cfg(test)]
mod test {
    use crate::btree2::slot::Either;

    use super::*;

    #[test]
    fn test_node() {
        let node = Node {
            t: NodeType::Leaf,
            is_root: true,
            len: 10,
            max: 20,
            next: -1,
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
}
