use std::sync::Mutex;

use bytes::BytesMut;

use crate::{
    disk::{Disk, FileSystem},
    page::{PageBuf, PageId},
    page_cache::{Result, SharedPageCache},
    table::node::Node,
    table::tuple::{RId, Tuple, TupleMeta},
    writep,
};

#[derive(Debug, Clone, Copy)]
pub struct TableMeta {
    first_page_id: PageId,
    last_page_id: PageId,
}

impl Default for TableMeta {
    fn default() -> Self {
        Self {
            first_page_id: -1,
            last_page_id: -1,
        }
    }
}

pub struct List<D: Disk = FileSystem> {
    pc: SharedPageCache<D>,
    first_page_id: PageId,
    last_page_id: Mutex<PageId>,
}

impl<D: Disk> List<D> {
    pub fn new(
        pc: SharedPageCache<D>,
        TableMeta {
            mut first_page_id,
            mut last_page_id,
        }: TableMeta,
    ) -> crate::Result<List<D>> {
        assert!(
            (first_page_id == -1 && last_page_id == -1)
                || (first_page_id != -1 && last_page_id != -1)
        );

        if first_page_id == -1 || last_page_id == -1 {
            let page = pc.new_page()?;
            first_page_id = page.id;
            last_page_id = page.id;
        }

        Ok(Self {
            pc,
            first_page_id,
            last_page_id: Mutex::new(last_page_id),
        })
    }

    pub fn default(pc: SharedPageCache<D>) -> crate::Result<List<D>> {
        let page = pc.new_page()?;
        let first_page_id = page.id;
        let last_page_id = page.id;
        drop(page);

        Ok(Self {
            pc,
            first_page_id,
            last_page_id: Mutex::new(last_page_id),
        })
    }

    fn last_page_id(&self) -> PageId {
        *self.last_page_id.lock().expect("todo")
    }

    fn last_page_id_mut(&self) -> std::sync::MutexGuard<'_, PageId> {
        self.last_page_id.lock().expect("todo")
    }

    pub fn iter(&self) -> Result<Iter<'_, D>> {
        let last_page_id = self.last_page_id();
        let page = self.pc.fetch_page(last_page_id)?;
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        Ok(Iter {
            list: self,
            r_id: RId {
                page_id: self.first_page_id,
                slot_id: 0,
            },
            end: RId {
                page_id: last_page_id,
                slot_id: node.len(),
            },
        })
    }

    pub fn insert(&self, tuple_data: &BytesMut, meta: &TupleMeta) -> Result<Option<RId>> {
        let mut last_page_id = self.last_page_id_mut();
        let page = self.pc.fetch_page(*last_page_id)?;
        let mut page_w = page.write();
        let mut node = Node::from(&page_w.data);

        if let Some(slot_id) = node.insert(tuple_data, meta) {
            writep!(page_w, &PageBuf::from(&node));
            return Ok(Some(RId {
                page_id: *last_page_id,
                slot_id,
            }));
        }

        if node.len() == 0 {
            todo!("tuple too large error")
        }

        // Insert into a new page and set the next pointer
        let npage = self.pc.new_page()?;
        let mut npage_w = npage.write();
        node.next_page_id = npage.id;
        *last_page_id = npage.id;

        // Write the next page id on first node
        // TODO: just write the page id instead of the entire page?
        writep!(page_w, &PageBuf::from(&node));

        let mut node = Node::from(&npage_w.data);
        match node.insert(tuple_data, meta) {
            Some(slot_id) => {
                writep!(npage_w, &PageBuf::from(&node));
                Ok(Some(RId {
                    page_id: *last_page_id,
                    slot_id,
                }))
            }
            None => unreachable!(),
        }
    }

    pub fn get(&self, r_id: RId) -> Result<Option<(TupleMeta, Tuple)>> {
        let page = self.pc.fetch_page(r_id.page_id)?;
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        let mut tuple = node.get(&r_id);
        if let Some((_, tuple)) = &mut tuple {
            tuple.rid = r_id;
        }

        Ok(tuple)
    }

    pub fn update(&mut self, _meta: &TupleMeta) -> Result<()> {
        todo!()
    }
}

// Iter should hold a read lock and deserialised page?
pub struct Iter<'a, D: Disk = FileSystem> {
    list: &'a List<D>,
    r_id: RId,
    end: RId,
}

impl<'a, D: Disk> Iterator for Iter<'a, D> {
    type Item = Result<(TupleMeta, Tuple)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end == self.r_id {
            return None;
        }

        let result = match self.list.get(self.r_id) {
            Ok(opt) => match opt {
                Some(t) => Ok(t),
                None => return None,
            },
            Err(e) => Err(e),
        };

        let page = match self.list.pc.fetch_page(self.r_id.page_id) {
            Ok(p) => p,
            Err(e) => return Some(Err(e)),
        };
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        if self.r_id.page_id == self.end.page_id && self.r_id.slot_id == self.end.slot_id - 1 {
            // Last tuple, increment (so the next iteration returns None) and return result
            self.r_id.slot_id += 1;
            return Some(result);
        } else if self.r_id.slot_id + 1 < node.len() {
            self.r_id.slot_id += 1;
        } else if node.next_page_id == 0 {
            return None;
        } else {
            self.r_id = RId {
                page_id: node.next_page_id,
                slot_id: 0,
            }
        }

        Some(result)
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use crate::{
        disk::Memory,
        page::PAGE_SIZE,
        page_cache::PageCache,
        replacer::LRU,
        table::list::List,
        table::{
            list::TableMeta,
            tuple::{RId, Tuple, TupleMeta},
        },
    };

    #[test]
    fn test_table() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 1;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let list = List::default(pc.clone())?;
        let meta = TupleMeta { deleted: false };
        let tuple_a = BytesMut::from(&std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8)[..]);
        let tuple_b = BytesMut::from(&std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8)[..]);

        let r_id_a = list.insert(&tuple_a, &meta)?.unwrap();
        let r_id_b = list.insert(&tuple_b, &meta)?.unwrap();

        let list = List::new(
            pc,
            TableMeta {
                first_page_id: list.first_page_id,
                last_page_id: list.last_page_id(),
            },
        )?;

        let (_, have_a) = list.get(r_id_a)?.unwrap();
        let (_, have_b) = list.get(r_id_b)?.unwrap();

        assert_eq!(tuple_a, have_a.data);
        assert_eq!(tuple_b, have_b.data);

        Ok(())
    }

    #[test]
    fn test_iter() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 4;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let first_page_id = pc.new_page()?.id;
        let list = List::new(
            pc.clone(),
            TableMeta {
                first_page_id,
                last_page_id: first_page_id,
            },
        )?;

        const WANT_LEN: usize = 100;
        let meta = TupleMeta { deleted: false };
        let mut tuples = Vec::new();
        for i in 0..WANT_LEN {
            let tuple = BytesMut::from(&std::array::from_fn::<u8, 150, _>(|j| (j * i) as u8)[..]);
            list.insert(&tuple, &meta)?;
            tuples.push(tuple);
        }

        let have = list
            .iter()?
            .enumerate()
            .collect::<Vec<(usize, crate::Result<(TupleMeta, Tuple)>)>>();

        assert_eq!(have.len(), WANT_LEN);

        for (i, result) in have {
            let (_, tuple) = result?;

            assert_eq!(tuples[i], tuple.data)
        }

        Ok(())
    }
}
