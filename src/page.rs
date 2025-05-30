use std::ops::Range;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::catalog::schema::Schema;

pub const PAGE_SIZE: usize = 4 * 1024;

pub type PageID = i32;
pub type PageBuf = [u8; PAGE_SIZE];
pub type PageReadGuard<'a> = RwLockReadGuard<'a, PageInner>;
pub type PageWriteGuard<'a> = RwLockWriteGuard<'a, PageInner>;

pub trait DiskObject {
    fn serialise(&self) -> PageBuf;
    fn deserialise(buf: PageBuf, schema: &Schema) -> Self;
}

pub struct ObjectReadGuard<'a, T> {
    _guard: RwLockReadGuard<'a, PageInner>,
    data: T,
}

impl<'a, T> std::ops::Deref for ObjectReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub struct ObjectWriteGuard<'a, T: DiskObject> {
    guard: RwLockWriteGuard<'a, PageInner>,
    data: T,
}

impl<'a, T> Drop for ObjectWriteGuard<'a, T>
where
    T: DiskObject,
{
    fn drop(&mut self) {
        self.guard.put(&self.data);
    }
}

impl<'a, T> std::ops::Deref for ObjectWriteGuard<'a, T>
where
    T: DiskObject,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> std::ops::DerefMut for ObjectWriteGuard<'a, T>
where
    T: DiskObject,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

pub struct Page(RwLock<PageInner>);

impl Default for Page {
    fn default() -> Self {
        let inner = PageInner::default();

        Self(RwLock::new(inner))
    }
}

impl Page {
    pub fn read(&self) -> PageReadGuard {
        self.0.read().expect("todo")
    }

    pub fn write(&self) -> PageWriteGuard {
        self.0.write().expect("todo")
    }

    pub fn read_object<T>(&self, schema: &Schema) -> ObjectReadGuard<T>
    where
        T: DiskObject,
    {
        let guard = self.0.read().unwrap();
        let data: T = DiskObject::deserialise(guard.data, schema);
        ObjectReadGuard { _guard: guard, data }
    }

    pub fn write_object<T>(&self, schema: &Schema) -> ObjectWriteGuard<T>
    where
        T: DiskObject,
    {
        let guard = self.0.write().unwrap();
        let data: T = DiskObject::deserialise(guard.data, schema);
        ObjectWriteGuard { guard, data }
    }
}

pub struct PageInner {
    pub id: PageID,
    pub dirty: bool,
    pub data: PageBuf,
}

impl Default for PageInner {
    fn default() -> Self {
        Self { id: -1, dirty: false, data: [0; PAGE_SIZE] }
    }
}

impl PageInner {
    pub fn put(&mut self, data: &impl DiskObject) {
        self.put_range(&data.serialise(), 0..PAGE_SIZE);
    }

    pub fn put_range(&mut self, data: &[u8], range: Range<usize>) {
        self.data[range].copy_from_slice(data);
        self.dirty = true;
    }

    pub fn reset(&mut self) {
        self.id = 0;
        self.dirty = false;
        self.data.fill(0);
    }
}
