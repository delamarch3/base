use bytes::{Buf, BufMut, BytesMut};

pub type PageID = u32;
pub const DEFAULT_PAGE_SIZE: usize = 4 * 1024;

/// A TupleID is composed of a PageID and the slot offset within that page
pub type TupleID = (PageID, u64);

pub struct Page<const SIZE: usize> {
    pub id: PageID,
    pin: u32,
    dirty: bool,
    pub data: BytesMut,
}

macro_rules! get_u64 {
    ($x:expr, $o:expr) => {
        u64::from_be_bytes($x[$o as usize..$o as usize + 8].try_into().unwrap())
    };
}

macro_rules! get_bytes {
    ($x:expr, $o:expr, $l:expr) => {
        &$x[$o as usize..$o as usize + $l as usize]
    };
}

macro_rules! put_bytes {
    ($dst:expr, $src:expr, $o:expr, $l:expr) => {
        $dst[$o as usize..$o as usize + $l as usize].copy_from_slice(&$src);
    };
}

impl<const SIZE: usize> Page<SIZE> {
    pub fn new(id: PageID) -> Self {
        let mut data = BytesMut::zeroed(SIZE);

        let header = Header {
            upper: Header::SIZE,
            lower: SIZE as u64,
            special: SIZE as u64,
        };
        put_bytes!(data, header.as_bytes(), 0, Header::SIZE);

        Self {
            id,
            pin: 0,
            dirty: false,
            data,
        }
    }

    pub fn from_bytes(id: PageID, data: BytesMut) -> Self {
        Self {
            id,
            pin: 0,
            dirty: false,
            data,
        }
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn inc_pin(&mut self) {
        self.pin += 1;
    }

    pub fn dec_pin(&mut self) {
        if self.pin > 0 {
            self.pin -= 1;
        }
    }

    pub fn read_tuple(&self, slot_offset: u64, schema: &[Type]) -> Tuple {
        let tuple_offset = get_u64!(self.data, slot_offset);
        let tuple_size = get_u64!(self.data, slot_offset + 8);
        let tuple_bytes = get_bytes!(self.data, tuple_offset, tuple_size);

        Tuple::parse(BytesMut::from(tuple_bytes), schema)
    }

    pub fn write_tuple(&mut self, tuple: &Tuple) -> TupleID {
        let mut header = Header::read(&self.data);

        // Write to slot array
        let slot_offset = header.upper;

        let len = tuple.len() as u64;
        let len_bytes = len.to_be_bytes();
        let tuple_offset = header.lower - len;
        let tuple_offset_bytes = tuple_offset.to_be_bytes();

        put_bytes!(self.data, tuple_offset_bytes, slot_offset, 8);
        put_bytes!(self.data, len_bytes, slot_offset + 8, 8);
        header.upper += TupleSlot::SIZE;

        // Write tuple
        put_bytes!(self.data, tuple.as_bytes(), tuple_offset, len);
        header.lower -= len;
        self.dirty = true;

        // Update header
        put_bytes!(self.data, header.as_bytes(), 0, Header::SIZE);

        (self.id, slot_offset)
    }
}

pub struct Header {
    /// Start of free space (end of slotted array)
    pub upper: u64,
    /// End of free space (start of rows)
    pub lower: u64,
    // Start of special data (equal to page size if none)
    pub special: u64,
}

impl Header {
    const SIZE: u64 = 24;

    pub fn read(data: &[u8]) -> Self {
        let upper = get_u64!(data, 0);
        let lower = get_u64!(data, 8);
        let special = get_u64!(data, 16);

        Self {
            upper,
            lower,
            special,
        }
    }

    pub fn as_bytes(&self) -> BytesMut {
        let mut ret = BytesMut::with_capacity(Self::SIZE as usize);
        ret.put_u64(self.upper);
        ret.put_u64(self.lower);
        ret.put_u64(self.special);

        ret
    }
}

pub struct TupleSlot {
    // offset: u64,
    // size: u64,
}

impl TupleSlot {
    const SIZE: u64 = 16;
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
    use super::{ColumnType, Header, Page, Tuple, TupleSlot, Type, DEFAULT_PAGE_SIZE};

    #[test]
    fn test_rw_page() {
        let mut page: Page<DEFAULT_PAGE_SIZE> = Page::new(0);

        let Header {
            upper,
            lower,
            special,
        } = Header::read(&page.data);
        assert!(
            upper == Header::SIZE
                && lower == DEFAULT_PAGE_SIZE as u64
                && special == DEFAULT_PAGE_SIZE as u64
        );

        let schema = [Type::Int32, Type::String, Type::Float32];
        let tuple_a = Tuple(vec![
            ColumnType::Int32(44),
            ColumnType::String("Hello world".into()),
            ColumnType::Float32(4.4),
        ]);
        let (_page_id, offset_a) = page.write_tuple(&tuple_a);

        let tuple_b = Tuple(vec![
            ColumnType::Int32(66),
            ColumnType::String("String".into()),
            ColumnType::Float32(6.6),
        ]);
        let (_page_id, offset_b) = page.write_tuple(&tuple_b);

        let read_tuple_a = page.read_tuple(offset_a, &schema);
        assert!(read_tuple_a == tuple_a);

        let read_tuple_b = page.read_tuple(offset_b, &schema);
        assert!(read_tuple_b == tuple_b);

        let Header {
            upper,
            lower,
            special,
        } = Header::read(&page.data);

        assert!(
            upper == Header::SIZE + (TupleSlot::SIZE * 2)
                && lower == DEFAULT_PAGE_SIZE as u64 - (tuple_a.len() + tuple_b.len()) as u64
                && special == DEFAULT_PAGE_SIZE as u64
        );
        assert!(page.dirty);
    }
}
