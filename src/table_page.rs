use std::ops::Range;

use bytes::BytesMut;

use crate::page::{PageBuf, PageId, PAGE_SIZE};

/*
    TablePage:
    NextPageID | NumTuples | NumDeletedTuples | Slots | Free | Tuples

    Slot:
    TupleInfo

    Tuple:
    RId | Data
*/

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct RId {
    page_id: PageId,
    slot_idx: u32,
}

#[derive(Debug, PartialEq)]
pub struct Tuple {
    r_id: RId,
    data: BytesMut,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct TupleMeta {
    deleted: bool,
}

impl From<&[u8]> for TupleMeta {
    fn from(value: &[u8]) -> Self {
        let deleted = u8::from_be_bytes(value[0..1].try_into().unwrap()) > 1;

        Self { deleted }
    }
}

const OFFSET: Range<usize> = 0..4;
const LEN: Range<usize> = 4..8;
const META: Range<usize> = 8..Slot::SIZE;

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Slot {
    offset: u32,
    len: u32,
    meta: TupleMeta,
}

impl From<&[u8]> for Slot {
    fn from(buf: &[u8]) -> Self {
        let offset = u32::from_be_bytes(buf[OFFSET].try_into().unwrap());
        let len = u32::from_be_bytes(buf[LEN].try_into().unwrap());
        let meta = TupleMeta::from(&buf[META]);

        Self { offset, len, meta }
    }
}

type TupleInfoBuf = [u8; Slot::SIZE];
impl From<&Slot> for TupleInfoBuf {
    fn from(value: &Slot) -> Self {
        let mut ret = [0; Slot::SIZE];

        ret[OFFSET].copy_from_slice(&value.offset.to_be_bytes());
        ret[LEN].copy_from_slice(&value.len.to_be_bytes());
        ret[META].copy_from_slice(&[value.meta.deleted as u8]);

        ret
    }
}

impl Slot {
    const SIZE: usize = 9;
}

const NEXT_PAGE_ID: Range<usize> = 0..4;
const TUPLES_LEN: Range<usize> = 4..8;
const DELETED_TUPLES_LEN: Range<usize> = 8..12;
const SLOTS_START: usize = 12;

#[derive(Debug, PartialEq)]
pub struct Table {
    page_start: *mut u8,
    next_page_id: PageId,
    tuples_len: u32,
    deleted_tuples_len: u32,
    slots: Vec<Slot>,
}

impl From<&PageBuf> for Table {
    fn from(buf: &PageBuf) -> Self {
        let page_start = buf.as_ptr() as *mut u8;
        let next_page_id = i32::from_be_bytes(buf[NEXT_PAGE_ID].try_into().unwrap());
        let tuples_len = u32::from_be_bytes(buf[TUPLES_LEN].try_into().unwrap());
        let deleted_tuples_len = u32::from_be_bytes(buf[DELETED_TUPLES_LEN].try_into().unwrap());

        let mut slots = Vec::new();
        const SLOT_SIZE: usize = Slot::SIZE;
        let left = &buf[SLOTS_START..];
        let mut from = 0;
        let mut rem = tuples_len;
        while rem > 0 {
            let bytes = &left[from..from + SLOT_SIZE];
            let slot = Slot::from(bytes);
            slots.push(slot);
            from = from + SLOT_SIZE;
            rem -= 1;
        }

        Self {
            page_start,
            next_page_id,
            tuples_len,
            deleted_tuples_len,
            slots,
        }
    }
}

impl From<&Table> for PageBuf {
    fn from(table: &Table) -> Self {
        let mut ret: PageBuf = [0; PAGE_SIZE];

        ret[NEXT_PAGE_ID].copy_from_slice(&table.next_page_id.to_be_bytes());
        ret[TUPLES_LEN].copy_from_slice(&table.tuples_len.to_be_bytes());
        ret[DELETED_TUPLES_LEN].copy_from_slice(&table.deleted_tuples_len.to_be_bytes());

        const SLOT_SIZE: usize = Slot::SIZE;
        let mut from = SLOTS_START;
        for slot in &table.slots {
            let slot = TupleInfoBuf::from(slot);
            ret[from..from + SLOT_SIZE].copy_from_slice(&slot);
            from += SLOT_SIZE;
        }

        let offset = match table.slots.last() {
            Some(o) => o.offset as usize,
            None => return ret,
        };
        assert!(offset < PAGE_SIZE);

        unsafe {
            let tuples_ptr = table.page_start.add(offset);
            let tuples = std::slice::from_raw_parts(tuples_ptr, PAGE_SIZE - offset);
            ret[offset..].copy_from_slice(tuples);
        }

        ret
    }
}

impl Table {
    const HEADER_SIZE: usize = 12;

    pub fn next_tuple_offset(&self, tuple: &Tuple) -> Option<usize> {
        let offset = match self.slots.last() {
            Some(slot) => slot.offset as usize,
            None => PAGE_SIZE,
        };

        let tuple_offset = offset - tuple.data.len();

        // Ensure tuple isn't written over header/slots
        let size = Self::HEADER_SIZE + Slot::SIZE * (self.tuples_len as usize + 1);
        if tuple_offset < size {
            return None;
        }

        Some(tuple_offset)
    }

    pub fn insert(&mut self, tuple: &Tuple, meta: &TupleMeta) -> Option<u32> {
        let offset = self.next_tuple_offset(tuple)?;
        let tuple_id = self.tuples_len;
        self.slots.push(Slot {
            offset: offset as u32,
            len: tuple.data.len() as u32,
            meta: *meta,
        });
        self.tuples_len += 1;

        unsafe {
            let tuples_ptr = self.page_start.add(offset);
            let tuples = std::slice::from_raw_parts_mut(tuples_ptr, PAGE_SIZE - offset);
            tuples[..tuple.data.len()].copy_from_slice(&tuple.data);
        }

        Some(tuple_id)
    }

    pub fn get(&self, r_id: &RId) -> (TupleMeta, Tuple) {
        let slot_idx = r_id.slot_idx;
        if slot_idx > self.tuples_len {
            todo!()
        }

        let Slot { offset, len, meta } = self.slots[slot_idx as usize];
        let mut tuple = Tuple {
            r_id: *r_id,
            data: BytesMut::zeroed(len as usize),
        };

        unsafe {
            let tuple_ptr = self.page_start.add(offset as usize);
            let tuple_data = std::slice::from_raw_parts(tuple_ptr, len as usize);
            tuple.data[..].copy_from_slice(tuple_data);
        }

        (meta, tuple)
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use crate::{
        page::{PageBuf, PAGE_SIZE},
        table_page::{RId, Slot, Table, Tuple, TupleMeta},
    };

    #[test]
    fn test_from() {
        let mut buf = [0; PAGE_SIZE];

        let tuple_a = std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8);
        let tuple_b = std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8);

        buf[PAGE_SIZE - 10..].copy_from_slice(&tuple_a);
        buf[PAGE_SIZE - 25..PAGE_SIZE - 10].copy_from_slice(&tuple_b);

        let mut table = Table {
            page_start: buf.as_mut_ptr(),
            next_page_id: 10,
            tuples_len: 2,
            deleted_tuples_len: 0,
            slots: vec![
                Slot {
                    offset: (PAGE_SIZE - 10) as u32,
                    len: 10,
                    meta: TupleMeta { deleted: false },
                },
                Slot {
                    offset: (PAGE_SIZE - 25) as u32,
                    len: 15,
                    meta: TupleMeta { deleted: false },
                },
            ],
        };

        let bytes = PageBuf::from(&table);

        let mut table2 = Table::from(&bytes);

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

        let mut table = Table {
            page_start: buf.as_mut_ptr(),
            next_page_id: 0,
            tuples_len: 0,
            deleted_tuples_len: 0,
            slots: Vec::new(),
        };

        let meta = TupleMeta { deleted: false };

        let r_id_a = RId {
            page_id: 0,
            slot_idx: 0,
        };
        let tuple_a = Tuple {
            r_id: r_id_a,
            data: BytesMut::from(&std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8)[..]),
        };

        let r_id_b = RId {
            page_id: 0,
            slot_idx: 1,
        };
        let tuple_b = Tuple {
            r_id: r_id_b,
            data: BytesMut::from(&std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8)[..]),
        };

        table.insert(&tuple_a, &meta);
        table.insert(&tuple_b, &meta);

        let (_, have_a) = table.get(&r_id_a);
        let (_, have_b) = table.get(&r_id_b);
        assert_eq!(tuple_a, have_a);
        assert_eq!(tuple_b, have_b)
    }
}
