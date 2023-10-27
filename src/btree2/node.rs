use std::{collections::BTreeSet, io::Cursor};

use bytes::{Buf, BytesMut};

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
        let mut cursor = Cursor::new(value);

        let t = NodeType::from(cursor.get_u8());
        let is_root = cursor.get_u8() > 0;
        let len = cursor.get_u32();
        let max = cursor.get_u32();
        let next = cursor.get_i32();

        let mut values = BTreeSet::new();
        let size = Slot::<K, V>::SIZE;

        let left = &cursor.get_ref()[14..];
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

        ret[0] = u8::from(node.t);
        ret[1] = node.is_root as u8;
        ret[2..6].copy_from_slice(&node.len.to_be_bytes());
        ret[6..10].copy_from_slice(&node.max.to_be_bytes());
        ret[10..14].copy_from_slice(&node.next.to_be_bytes());

        let size = Slot::<K, V>::SIZE;
        let mut from = 14;
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
    fn test_node_rw() {
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
