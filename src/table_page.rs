use std::mem::size_of;

use tokio::sync::RwLockWriteGuard;

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    get_bytes, get_u64,
    page::{Page, PageID, SharedPage},
    put_bytes,
};

/// A TupleID is composed of a PageID and the slot offset within that page
pub type TupleID = (PageID, u64);
pub const TUPLE_SLOT_SIZE: u64 = 16;

pub fn new_shared<const SIZE: usize>(id: PageID) -> SharedPage<SIZE> {
    let mut data = [0; SIZE];

    let header = Header {
        upper: Header::SIZE,
        lower: SIZE as u64,
    };
    put_bytes!(data, header.as_bytes(), 0, Header::SIZE);

    SharedPage::from_bytes(id, data)
}

pub fn init<const SIZE: usize>(mut page: RwLockWriteGuard<'_, Page<SIZE>>) {
    let header = Header {
        upper: Header::SIZE,
        lower: SIZE as u64,
    };

    put_bytes!(page.data, header.as_bytes(), 0, Header::SIZE);
}

// TODO: Should take lock guards?
pub async fn read_tuple(page: &SharedPage, slot_offset: u64, schema: &[Type]) -> Tuple {
    let data = &page.read().await.data;
    let tuple_offset = get_u64!(data, slot_offset);
    let tuple_size = get_u64!(data, slot_offset + 8);
    let tuple_bytes = get_bytes!(data, tuple_offset, tuple_size);

    Tuple::parse(BytesMut::from(tuple_bytes), schema)
}

pub async fn write_tuple(page: &SharedPage, tuple: &Tuple) -> TupleID {
    let mut page = page.write().await;
    let mut header = Header::read(&page.data);

    // Write to slot array
    let slot_offset = header.upper;

    let len = tuple.len() as u64;
    let len_bytes = len.to_be_bytes();
    let tuple_offset = header.lower - len;
    let tuple_offset_bytes = tuple_offset.to_be_bytes();

    put_bytes!(page.data, tuple_offset_bytes, slot_offset, 8);
    put_bytes!(page.data, len_bytes, slot_offset + 8, 8);
    header.upper += TUPLE_SLOT_SIZE;

    // Write tuple
    put_bytes!(page.data, tuple.as_bytes(), tuple_offset, len);
    header.lower -= len;
    page.dirty = true;

    // Update header
    put_bytes!(page.data, header.as_bytes(), 0, Header::SIZE);

    (page.id, slot_offset)
}

pub struct Header {
    /// Start of free space (end of slotted array)
    pub upper: u64,
    /// End of free space (start of rows)
    pub lower: u64,
}

impl Header {
    pub const SIZE: u64 = 2 * size_of::<u64>() as u64;

    pub fn read(data: &[u8]) -> Self {
        let upper = get_u64!(data, 0);
        let lower = get_u64!(data, 8);

        Self { upper, lower }
    }

    pub fn as_bytes(&self) -> BytesMut {
        let mut ret = BytesMut::with_capacity(Self::SIZE as usize);
        ret.put_u64(self.upper);
        ret.put_u64(self.lower);

        ret
    }
}

pub enum Type {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Bool,
    // Size of dropped column, 0 for string?
    Dropped(u8),
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum ColumnType {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(BytesMut),
    Bool(bool),
}

impl ColumnType {
    #[inline]
    pub fn parse(bytes: &mut BytesMut, schema_type: &Type) -> Option<Self> {
        match schema_type {
            Type::Int32 => Some(ColumnType::Int32(bytes.get_i32())),
            Type::Int64 => Some(ColumnType::Int64(bytes.get_i64())),
            Type::Float32 => Some(ColumnType::Float32(bytes.get_f32())),
            Type::Float64 => Some(ColumnType::Float64(bytes.get_f64())),
            Type::String => {
                let len = bytes.get_u64() as usize;
                let start = bytes.len() - bytes.remaining();
                let end = start + len;
                let s = BytesMut::from(&bytes[start..end]);
                bytes.advance(len);

                Some(ColumnType::String(s))
            }
            Type::Bool => Some(ColumnType::Bool(bytes.get_u8() != 0)),
            Type::Dropped(len) if *len == 0 => {
                // String
                let len = bytes.get_u64();
                bytes.advance(len as usize);

                None
            }
            Type::Dropped(len) => {
                bytes.advance(*len as usize);

                None
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Tuple(pub Vec<ColumnType>);

impl Tuple {
    #[inline]
    pub fn parse(mut bytes: BytesMut, schema: &[Type]) -> Self {
        let mut ret = Vec::with_capacity(schema.len());

        for st in schema {
            if let Some(rt) = ColumnType::parse(&mut bytes, st) {
                ret.push(rt);
            }
        }

        Self(ret)
    }

    #[inline]
    fn len(&self) -> usize {
        let mut len = 0;

        for v in &self.0 {
            match v {
                ColumnType::Int32(_) | ColumnType::Float32(_) => len += 4,
                ColumnType::Int64(_) | ColumnType::Float64(_) => len += 8,
                ColumnType::String(s) => len += s.len() + 8,
                ColumnType::Bool(_) => len += 1,
            }
        }

        len
    }

    #[inline]
    fn as_bytes(&self) -> BytesMut {
        assert!(self.0.len() == self.0.capacity());
        let mut bytes = BytesMut::with_capacity(self.len());

        for v in &self.0 {
            match v {
                ColumnType::Int32(i) => {
                    bytes.put_i32(*i);
                }
                ColumnType::Int64(i) => {
                    bytes.put_i64(*i);
                }
                ColumnType::Float32(f) => {
                    bytes.put_f32(*f);
                }
                ColumnType::Float64(f) => {
                    bytes.put_f64(*f);
                }
                ColumnType::String(s) => {
                    bytes.put_u64(s.len() as u64);
                    bytes.extend_from_slice(s);
                }
                ColumnType::Bool(b) => {
                    bytes.put_u8(u8::from(*b));
                }
            }
        }

        bytes
    }
}

#[cfg(test)]
mod test {
    use crate::{
        page::DEFAULT_PAGE_SIZE,
        table_page::{self, ColumnType, Header, Tuple, Type, TUPLE_SLOT_SIZE},
    };

    #[tokio::test]
    async fn test_rw_page() {
        let page = table_page::new_shared(0);

        let Header { upper, lower } = Header::read(&page.read().await.data);
        assert!(upper == Header::SIZE && lower == DEFAULT_PAGE_SIZE as u64);

        let schema = [Type::Int32, Type::String, Type::Float32];
        let tuple_a = Tuple(vec![
            ColumnType::Int32(44),
            ColumnType::String("Hello world".into()),
            ColumnType::Float32(4.4),
        ]);
        let (_page_id, offset_a) = table_page::write_tuple(&page, &tuple_a).await;

        let tuple_b = Tuple(vec![
            ColumnType::Int32(66),
            ColumnType::String("String".into()),
            ColumnType::Float32(6.6),
        ]);
        let (_page_id, offset_b) = table_page::write_tuple(&page, &tuple_b).await;

        let read_tuple_a = table_page::read_tuple(&page, offset_a, &schema).await;
        assert!(read_tuple_a == tuple_a);

        let read_tuple_b = table_page::read_tuple(&page, offset_b, &schema).await;
        assert!(read_tuple_b == tuple_b);

        let Header { upper, lower } = Header::read(&page.read().await.data);

        assert!(
            upper == Header::SIZE + (TUPLE_SLOT_SIZE * 2)
                && lower == DEFAULT_PAGE_SIZE as u64 - (tuple_a.len() + tuple_b.len()) as u64 // && special == DEFAULT_PAGE_SIZE as u64
        );

        assert!(page.read().await.dirty);
    }
}
