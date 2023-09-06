use std::{collections::BinaryHeap, mem::size_of};

use bytes::BytesMut;
use tokio::sync::RwLockWriteGuard;

use crate::{
    btree::BTreeHeader,
    get_bytes, get_u32,
    page::{Page, PageID, DEFAULT_PAGE_SIZE},
    pair::{IndexStorable, Pair, Pair2, PairType, Storable},
    put_bytes,
    table_page::RelationID,
};

// pub struct LeafNode<K, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
//     header: BTreeHeader,
//     next_page_id: PageID,
//     pairs: BinaryHeap<Pair<K, RelationID>>,
// }

// impl<'a, const PAGE_SIZE: usize, K> LeafNode<K, PAGE_SIZE>
// where
//     PairType<K>: Into<BytesMut> + From<&'a [u8]> + Copy + Ord,
//     K: Ord,
// {
//     pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
//         let header = BTreeHeader::new(data);
//         let next_page_id = get_u32!(data, BTreeHeader::SIZE);

//         let k_size = size_of::<K>();
//         let v_size = size_of::<RelationID>();

//         let mut pairs = BinaryHeap::new();
//         let mut pos = BTreeHeader::SIZE;

//         while pos < PAGE_SIZE {
//             let k_bytes = get_bytes!(data, pos, k_size);
//             pos += k_bytes.len();
//             let v_bytes = get_bytes!(data, pos, v_size);
//             pos += v_bytes.len();

//             // Check invalid rel id
//             let rel_id: PairType<RelationID> = v_bytes.into();
//             if rel_id.0 .0 == 0 || rel_id.0 .1 == 0 {
//                 continue;
//             }

//             let key: PairType<K> = k_bytes.into();

//             // Pair::from_bytes funny behaviour here
//             pairs.push(Pair { a: key, b: rel_id });
//         }

//         Self {
//             header,
//             next_page_id,
//             pairs,
//         }
//     }

//     pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>) {
//         self.header.write_data(&mut page.data);
//         put_bytes!(
//             page.data,
//             self.next_page_id.to_be_bytes(),
//             BTreeHeader::SIZE,
//             size_of::<PageID>()
//         );

//         let mut pos = BTreeHeader::SIZE;
//         let p_size = size_of::<K>() + size_of::<PageID>();
//         for pair in &self.pairs {
//             if pos + p_size >= PAGE_SIZE {
//                 break;
//             }

//             let key: BytesMut = pair.a.into();
//             let value: BytesMut = pair.b.into();

//             put_bytes!(page.data, key, pos, key.len());
//             pos += key.len();
//             put_bytes!(page.data, value, pos, value.len());
//             pos += value.len();
//         }

//         page.dirty = true;
//     }

//     pub fn len(&self) -> usize {
//         self.pairs.len()
//     }

//     pub fn insert(&mut self, k: K, rel_id: RelationID) {
//         let pair = Pair {
//             a: PairType::new(k),
//             b: PairType::new(rel_id),
//         };

//         self.pairs.push(pair);
//     }
// }

pub struct LeafNode2<K, const PAGE_SIZE: usize = DEFAULT_PAGE_SIZE> {
    header: BTreeHeader,
    next_page_id: PageID,
    pairs: BinaryHeap<Pair2<K, RelationID>>,
}

impl<'a, const PAGE_SIZE: usize, K> LeafNode2<K, PAGE_SIZE>
where
    K: IndexStorable,
{
    pub fn new(data: &'a [u8; PAGE_SIZE]) -> Self {
        let header = BTreeHeader::new(data);
        let next_page_id = get_u32!(data, BTreeHeader::SIZE);

        let k_size = size_of::<K>();
        let v_size = size_of::<RelationID>();

        let mut pairs = BinaryHeap::new();
        let mut pos = BTreeHeader::SIZE;

        while pos < PAGE_SIZE {
            let k_bytes = get_bytes!(data, pos, k_size);
            pos += k_bytes.len();
            let v_bytes = get_bytes!(data, pos, v_size);
            pos += v_bytes.len();

            // Check invalid rel id
            let rel_id = <RelationID as Storable>::from_bytes(v_bytes);
            if rel_id.0 == 0 || rel_id.1 == 0 {
                continue;
            }

            let key = <K as Storable>::from_bytes(k_bytes);

            pairs.push(Pair2::new(key, rel_id));
        }

        Self {
            header,
            next_page_id,
            pairs,
        }
    }

    // pub fn write_data(&self, page: &mut RwLockWriteGuard<'_, Page<PAGE_SIZE>>) {
    //     self.header.write_data(&mut page.data);
    //     put_bytes!(
    //         page.data,
    //         self.next_page_id.to_be_bytes(),
    //         BTreeHeader::SIZE,
    //         size_of::<PageID>()
    //     );

    //     let mut pos = BTreeHeader::SIZE;
    //     let p_size = size_of::<K>() + size_of::<PageID>();
    //     for pair in &self.pairs {
    //         if pos + p_size >= PAGE_SIZE {
    //             break;
    //         }

    //         let key: BytesMut = pair.a.into();
    //         let value: BytesMut = pair.b.into();

    //         put_bytes!(page.data, key, pos, key.len());
    //         pos += key.len();
    //         put_bytes!(page.data, value, pos, value.len());
    //         pos += value.len();
    //     }

    //     page.dirty = true;
    // }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    // pub fn insert(&mut self, k: K, rel_id: RelationID) {
    //     let pair = Pair {
    //         a: PairType::new(k),
    //         b: PairType::new(rel_id),
    //     };

    //     self.pairs.push(pair);
    // }
}
