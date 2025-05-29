use std::marker::PhantomData;
use std::ops::Range;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub const PAGE_SIZE: usize = 4 * 1024;

pub type PageID = i32;
pub type PageBuf = [u8; PAGE_SIZE];
pub type PageReadGuard<'a> = RwLockReadGuard<'a, PageInner>;
pub type PageWriteGuard<'a> = RwLockWriteGuard<'a, PageInner>;

pub struct PageReadGuard2<'a, T> {
    pub guard: RwLockReadGuard<'a, PageInner>,
    data: *const T,
    _data: PhantomData<&'a T>,
}

impl<'a, T> std::ops::Deref for PageReadGuard2<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

pub struct PageWriteGuard2<'a, T> {
    pub guard: RwLockWriteGuard<'a, PageInner>,
    data: *mut T,
    _data: PhantomData<&'a mut T>,
}

impl<'a, T> std::ops::Deref for PageWriteGuard2<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}

impl<'a, T> std::ops::DerefMut for PageWriteGuard2<'a, T> {
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

pub struct PageWriteGuard3<'a, T>
where
    PageBuf: for<'b> From<&'b T>,
{
    guard: RwLockWriteGuard<'a, PageInner>,
    data: T,
}

impl<'a, T> Drop for PageWriteGuard3<'a, T>
where
    PageBuf: for<'b> From<&'b T>,
{
    fn drop(&mut self) {
        let buf: PageBuf = (&self.data).into();
        self.guard.put(buf);
        self.guard.dirty = true;
    }
}

impl<'a, T> std::ops::Deref for PageWriteGuard3<'a, T>
where
    PageBuf: for<'b> From<&'b T>,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> std::ops::DerefMut for PageWriteGuard3<'a, T>
where
    PageBuf: for<'b> From<&'b T>,
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
    pub fn read2<T>(&self) -> PageReadGuard2<T> {
        let guard = self.0.read().unwrap();
        let data = guard.data.as_ptr() as *const T;
        PageReadGuard2 { guard, data, _data: PhantomData }
    }

    pub fn write2<T>(&self) -> PageWriteGuard2<T> {
        let mut guard = self.0.write().unwrap();
        guard.dirty = true;
        let data = guard.data.as_mut_ptr() as *mut T;
        PageWriteGuard2 { guard, data, _data: PhantomData }
    }

    // A nicer interface compared to the current implementation
    pub fn read3<T>(&self) -> PageReadGuard3<T>
    where
        T: for<'b> From<&'b PageBuf>,
        PageBuf: for<'b> From<&'b T>,
    {
        let guard = self.0.read().unwrap();
        let data: T = (&guard.data).into();
        PageReadGuard3 { _guard: guard, data }
    }

    pub fn write3<T>(&self) -> PageWriteGuard3<T>
    where
        T: for<'b> From<&'b PageBuf>,
        PageBuf: for<'b> From<&'b T>,
    {
        let guard = self.0.write().unwrap();
        let data: T = (&guard.data).into();
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
