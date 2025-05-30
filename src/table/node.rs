use crate::page::{DiskObject, PageBuf, PageID, PAGE_SIZE};
use crate::storable::Storable;
use crate::table::tuple::Data as TupleData;

use bytes::BytesMut;
use std::ops::Range;

// TablePage:
// NextPageID | NumTuples | NumDeletedTuples | Slots | Free | Tuples
//
// Slot:
// TupleInfo
//
// Tuple:
// RID | Data

pub type SlotID = u32;

#[derive(Debug, PartialEq, Eq, Copy, Clone, Default)]
pub struct RID {
    pub page_id: PageID,
    pub slot_id: SlotID,
}

impl Storable for RID {
    const SIZE: usize = 8;

    type ByteArray = [u8; Self::SIZE];

    fn into_bytes(self) -> Self::ByteArray {
        let mut buf = [0; 8];
        buf[0..4].copy_from_slice(&self.page_id.into_bytes());
        buf[4..8].copy_from_slice(&self.slot_id.into_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let page_id = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let slot_id = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        Self { page_id, slot_id }
    }

    fn write_to(&self, dst: &mut [u8], pos: usize) {
        dst[pos..pos + Self::SIZE].copy_from_slice(&self.into_bytes());
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct TupleMeta {
    pub deleted: bool,
}

impl From<&[u8]> for TupleMeta {
    fn from(value: &[u8]) -> Self {
        let deleted = u8::from_be_bytes(value[0..1].try_into().unwrap()) > 1;
        Self { deleted }
    }
}

const OFFSET: Range<usize> = 0..4;
const LEN: Range<usize> = 4..8;
const META: Range<usize> = 8..TupleSlot::SIZE;

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct TupleSlot {
    pub offset: u32,
    pub len: u32,
    pub meta: TupleMeta,
}

impl From<&[u8]> for TupleSlot {
    fn from(buf: &[u8]) -> Self {
        let offset = u32::from_be_bytes(buf[OFFSET].try_into().unwrap());
        let len = u32::from_be_bytes(buf[LEN].try_into().unwrap());
        let meta = TupleMeta::from(&buf[META]);

        Self { offset, len, meta }
    }
}

impl TupleSlot {
    pub const SIZE: usize = 9;
}

pub type TupleInfoBuf = [u8; TupleSlot::SIZE];
impl From<&TupleSlot> for TupleInfoBuf {
    fn from(value: &TupleSlot) -> Self {
        let mut ret = [0; TupleSlot::SIZE];

        ret[OFFSET].copy_from_slice(&value.offset.to_be_bytes());
        ret[LEN].copy_from_slice(&value.len.to_be_bytes());
        ret[META].copy_from_slice(&[value.meta.deleted as u8]);

        ret
    }
}

const NEXT_PAGE_ID: Range<usize> = 0..4;
const TUPLES_LEN: Range<usize> = 4..8;
const DELETED_TUPLES_LEN: Range<usize> = 8..12;
const SLOTS_START: usize = 12;

#[derive(Debug, PartialEq)]
pub struct Node {
    page_start: *mut u8,
    pub next_page_id: PageID,
    deleted_tuples_len: u32,
    slots: Vec<TupleSlot>,
}

impl DiskObject for Node {
    fn serialise(&self) -> PageBuf {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[NEXT_PAGE_ID].copy_from_slice(&self.next_page_id.to_be_bytes());
        ret[TUPLES_LEN].copy_from_slice(&(self.slots.len() as u32).to_be_bytes());
        ret[DELETED_TUPLES_LEN].copy_from_slice(&self.deleted_tuples_len.to_be_bytes());

        const SLOT_SIZE: usize = TupleSlot::SIZE;
        let mut from = SLOTS_START;
        for slot in &self.slots {
            let slot = TupleInfoBuf::from(slot);
            ret[from..from + SLOT_SIZE].copy_from_slice(&slot);
            from += SLOT_SIZE;
        }

        let offset = match self.slots.last() {
            Some(o) => o.offset as usize,
            None => return ret,
        };
        assert!(offset < PAGE_SIZE, "tuple being written at PAGE_SIZE or greater");

        unsafe {
            let tuples_ptr = self.page_start.add(offset);
            let tuples = std::slice::from_raw_parts(tuples_ptr, PAGE_SIZE - offset);
            ret[offset..].copy_from_slice(tuples);
        }

        ret
    }

    fn deserialise(buf: PageBuf, _: &crate::catalog::schema::Schema) -> Self {
        let page_start = buf.as_ptr() as *mut u8;
        let next_page_id = i32::from_be_bytes(buf[NEXT_PAGE_ID].try_into().unwrap());
        let tuples_len = u32::from_be_bytes(buf[TUPLES_LEN].try_into().unwrap());
        let deleted_tuples_len = u32::from_be_bytes(buf[DELETED_TUPLES_LEN].try_into().unwrap());

        let mut slots = Vec::new();
        const SLOT_SIZE: usize = TupleSlot::SIZE;
        let left = &buf[SLOTS_START..];
        let mut from = 0;
        let mut rem = tuples_len;
        while rem > 0 {
            let bytes = &left[from..from + SLOT_SIZE];
            let slot = TupleSlot::from(bytes);
            slots.push(slot);
            from += SLOT_SIZE;
            rem -= 1;
        }

        Self { page_start, next_page_id, deleted_tuples_len, slots }
    }
}

impl Node {
    const HEADER_SIZE: usize = 12;

    pub fn len(&self) -> u32 {
        self.slots.len() as u32
    }

    pub fn next_tuple_offset(&self, tuple: &TupleData) -> Option<usize> {
        let offset = match self.slots.last() {
            Some(slot) => slot.offset as usize,
            None => PAGE_SIZE,
        };

        let tuple_offset = offset - tuple.size();

        // Ensure tuple isn't written over header/slots
        let size = Self::HEADER_SIZE + TupleSlot::SIZE * (self.len() as usize + 1);
        if tuple_offset < size {
            return None;
        }

        Some(tuple_offset)
    }

    pub fn insert(&mut self, tuple: &TupleData, meta: &TupleMeta) -> Option<u32> {
        let offset = self.next_tuple_offset(tuple)?;
        let slot_id = self.len();
        self.slots.push(TupleSlot { offset: offset as u32, len: tuple.size() as u32, meta: *meta });

        unsafe {
            let tuples_ptr = self.page_start.add(offset);
            let tuples = std::slice::from_raw_parts_mut(tuples_ptr, PAGE_SIZE - offset);
            tuples[..tuple.size()].copy_from_slice(&tuple.0);
        }

        Some(slot_id)
    }

    pub fn get(&self, rid: &RID) -> Option<(TupleMeta, TupleData)> {
        let slot_id = rid.slot_id;
        if slot_id > self.len() {
            todo!()
        }

        let TupleSlot { offset, len, meta } = self.slots[slot_id as usize];
        let mut tuple_dst = BytesMut::zeroed(len as usize);

        unsafe {
            let tuple_ptr = self.page_start.add(offset as usize);
            let tuple_src = std::slice::from_raw_parts(tuple_ptr, len as usize);
            tuple_dst[..].copy_from_slice(tuple_src);
        }

        Some((meta, TupleData(tuple_dst)))
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use crate::{
        catalog::schema::Schema,
        page::{DiskObject, PAGE_SIZE},
        table::node::{Node, TupleData, TupleMeta, TupleSlot, RID},
    };

    #[test]
    fn test_serde() {
        let mut buf = [0; PAGE_SIZE];

        let tuple_a = std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8);
        let tuple_b = std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8);

        buf[PAGE_SIZE - 10..].copy_from_slice(&tuple_a);
        buf[PAGE_SIZE - 25..PAGE_SIZE - 10].copy_from_slice(&tuple_b);

        let mut table = Node {
            page_start: buf.as_mut_ptr(),
            next_page_id: 10,
            deleted_tuples_len: 0,
            slots: vec![
                TupleSlot {
                    offset: (PAGE_SIZE - 10) as u32,
                    len: 10,
                    meta: TupleMeta { deleted: false },
                },
                TupleSlot {
                    offset: (PAGE_SIZE - 25) as u32,
                    len: 15,
                    meta: TupleMeta { deleted: false },
                },
            ],
        };

        let bytes = table.serialise();

        let mut table2 = Node::deserialise(bytes, &Schema::default());

        let offset = table.slots.last().unwrap().offset as usize;
        let tuples = unsafe {
            let tuples_ptr = table2.page_start.add(offset);
            std::slice::from_raw_parts(tuples_ptr, PAGE_SIZE - offset)
        };
        assert_eq!(&tuples[0..15], &tuple_b);
        assert_eq!(&tuples[15..], &tuple_a);

        table.page_start = std::ptr::null_mut();
        table2.page_start = std::ptr::null_mut();

        assert_eq!(table, table2);
    }

    #[test]
    fn test_insert() {
        let mut buf = [0; PAGE_SIZE];

        let mut table = Node {
            page_start: buf.as_mut_ptr(),
            next_page_id: 0,
            deleted_tuples_len: 0,
            slots: Vec::new(),
        };

        let meta = TupleMeta { deleted: false };

        let rid_a = RID { page_id: 0, slot_id: 0 };
        let want_a =
            TupleData(BytesMut::from(&std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8)[..]));

        let rid_b = RID { page_id: 0, slot_id: 1 };
        let want_b =
            TupleData(BytesMut::from(&std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8)[..]));

        table.insert(&want_a, &meta);
        table.insert(&want_b, &meta);

        let (_, have_a) = table.get(&rid_a).unwrap();
        let (_, have_b) = table.get(&rid_b).unwrap();
        assert_eq!(want_a, have_a);
        assert_eq!(want_b, have_b)
    }
}
