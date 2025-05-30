use std::marker::PhantomData;
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

pub struct PageReadGuardUnsafe<'a, T> {
    pub guard: RwLockReadGuard<'a, PageInner>,
    data: *const T,
    _data: PhantomData<&'a T>,
}

impl<'a, T> std::ops::Deref for PageReadGuardUnsafe<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

pub struct PageWriteGuardUnsafe<'a, T> {
    pub guard: RwLockWriteGuard<'a, PageInner>,
    data: *mut T,
    _data: PhantomData<&'a mut T>,
}

impl<'a, T> std::ops::Deref for PageWriteGuardUnsafe<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl<'a, T> std::ops::DerefMut for PageWriteGuardUnsafe<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.data }
    }
}

pub struct PageReadGuard3<'a, T> {
    _guard: RwLockReadGuard<'a, PageInner>,
    data: T,
}

impl<'a, T> std::ops::Deref for PageReadGuard3<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub struct PageWriteGuard3<'a, T: DiskObject> {
    guard: RwLockWriteGuard<'a, PageInner>,
    data: T,
}

impl<'a, T> Drop for PageWriteGuard3<'a, T>
where
    T: DiskObject,
{
    fn drop(&mut self) {
        let buf = self.data.serialise();
        self.guard.put(buf);
    }
}

impl<'a, T> std::ops::Deref for PageWriteGuard3<'a, T>
where
    T: DiskObject,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> std::ops::DerefMut for PageWriteGuard3<'a, T>
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

    // For objects that can be directly reinterpreted
    pub fn read2<T>(&self) -> PageReadGuardUnsafe<T> {
        let guard = self.0.read().unwrap();
        let data = guard.data.as_ptr() as *const T;
        PageReadGuardUnsafe { guard, data, _data: PhantomData }
    }

    pub fn write2<T>(&self) -> PageWriteGuardUnsafe<T> {
        let mut guard = self.0.write().unwrap();
        guard.dirty = true;
        let data = guard.data.as_mut_ptr() as *mut T;
        PageWriteGuardUnsafe { guard, data, _data: PhantomData }
    }

    // A nicer interface compared to the current implementation
    pub fn read3<T>(&self, schema: &Schema) -> PageReadGuard3<'_, T>
    where
        T: DiskObject,
    {
        let guard = self.0.read().unwrap();
        let data: T = DiskObject::deserialise(guard.data, schema);
        PageReadGuard3 { _guard: guard, data }
    }

    pub fn write3<'a, T>(&self, schema: &'a Schema) -> PageWriteGuard3<'_, T>
    where
        T: DiskObject,
    {
        let guard = self.0.write().unwrap();
        let data: T = DiskObject::deserialise(guard.data, schema);
        PageWriteGuard3 { guard, data }
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
    pub fn put(&mut self, data: impl Into<PageBuf>) {
        self.put_range(&data.into(), 0..PAGE_SIZE);
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
