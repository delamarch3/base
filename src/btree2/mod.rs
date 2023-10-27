pub mod node;
pub mod slot;

use std::marker::PhantomData;

use crate::{page::PageId, page_cache::SharedPageCache};

pub struct BTree<K, V> {
    root: PageId,
    pc: SharedPageCache,
    max: usize,
    _data: PhantomData<(K, V)>,
}
