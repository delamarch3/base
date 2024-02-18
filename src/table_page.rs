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

pub struct RId {
    page_id: PageId,
    slot_idx: u32,
}

pub struct Tuple {
    r_id: RId,
    data: BytesMut,
}

#[derive(Debug, PartialEq)]
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
const META: Range<usize> = 8..TupleInfo::SIZE;

#[derive(Debug, PartialEq)]
pub struct TupleInfo {
    offset: u32,
    len: u32,
    meta: TupleMeta,
}

impl From<&[u8]> for TupleInfo {
    fn from(buf: &[u8]) -> Self {
        let offset = u32::from_be_bytes(buf[OFFSET].try_into().unwrap());
        let len = u32::from_be_bytes(buf[LEN].try_into().unwrap());
        let meta = TupleMeta::from(&buf[META]);

        Self { offset, len, meta }
    }
}

type TupleInfoBuf = [u8; TupleInfo::SIZE];
impl From<&TupleInfo> for TupleInfoBuf {
    fn from(value: &TupleInfo) -> Self {
        let mut ret = [0; TupleInfo::SIZE];

        ret[OFFSET].copy_from_slice(&value.offset.to_be_bytes());
        ret[LEN].copy_from_slice(&value.len.to_be_bytes());
        ret[META].copy_from_slice(&[value.meta.deleted as u8]);

        ret
    }
}

impl TupleInfo {
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
    slots: Vec<TupleInfo>,
}

impl From<&PageBuf> for Table {
    fn from(buf: &PageBuf) -> Self {
        let page_start = buf.as_ptr() as *mut u8;
        let next_page_id = i32::from_be_bytes(buf[NEXT_PAGE_ID].try_into().unwrap());
        let tuples_len = u32::from_be_bytes(buf[TUPLES_LEN].try_into().unwrap());
        let deleted_tuples_len = u32::from_be_bytes(buf[DELETED_TUPLES_LEN].try_into().unwrap());

        let mut slots = Vec::new();
        const SLOT_SIZE: usize = TupleInfo::SIZE;
        let left = &buf[SLOTS_START..];
        let mut from = 0;
        let mut rem = tuples_len;
        while rem > 0 {
            let bytes = &left[from..from + SLOT_SIZE];
            let slot = TupleInfo::from(bytes);
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

        const SLOT_SIZE: usize = TupleInfo::SIZE;
        let mut from = SLOTS_START;
        for slot in &table.slots {
            let slot = TupleInfoBuf::from(slot);
            ret[from..from + SLOT_SIZE].copy_from_slice(&slot);
            from += SLOT_SIZE;
        }

        unsafe {
            let offset = match table.slots.last() {
                Some(o) => o.offset as usize,
                None => return ret,
            };
            assert!(offset < PAGE_SIZE);

            let tuples_ptr = table.page_start.add(offset);
            let tuples = std::slice::from_raw_parts(tuples_ptr, PAGE_SIZE - offset);
            ret[offset..].copy_from_slice(tuples);
        }

        ret
    }
}

impl Table {
    pub fn next_tuple_offset(&self) {}
}

#[cfg(test)]
mod test {
    use crate::{
        page::{PageBuf, PAGE_SIZE},
        table_page::Table,
    };

    use super::{TupleInfo, TupleMeta};

    #[test]
    fn test_from() {
        let mut buf = [0; PAGE_SIZE];

        let tuple_a = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20];
        let tuple_b = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75];

        buf[PAGE_SIZE - 10..].copy_from_slice(&tuple_a);
        buf[PAGE_SIZE - 25..PAGE_SIZE - 10].copy_from_slice(&tuple_b);

        let mut table = Table {
            page_start: buf.as_mut_ptr(),
            next_page_id: 10,
            tuples_len: 2,
            deleted_tuples_len: 0,
            slots: vec![
                TupleInfo {
                    offset: (PAGE_SIZE - 10) as u32,
                    len: 10,
                    meta: TupleMeta { deleted: false },
                },
                TupleInfo {
                    offset: (PAGE_SIZE - 25) as u32,
                    len: 15,
                    meta: TupleMeta { deleted: false },
                },
            ],
        };

        let bytes = PageBuf::from(&table);

        let mut table2 = Table::from(&bytes);

        unsafe {
            let offset = table.slots.last().unwrap().offset as usize;
            let tuples_ptr = table2.page_start.add(offset);
            let tuples = std::slice::from_raw_parts(tuples_ptr, PAGE_SIZE - offset);
            assert_eq!(&tuples[0..15], &tuple_b);
            assert_eq!(&tuples[15..], &tuple_a);
        }

        table.page_start = std::ptr::null_mut();
        table2.page_start = std::ptr::null_mut();

        assert_eq!(table, table2);
    }
}
