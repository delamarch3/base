use std::sync::{Arc, Mutex};

use crate::page::{PageBuf, PageID};
use crate::page_cache::{Result, SharedPageCache};
use crate::table::node::Node;
use crate::table::node::{TupleMeta, RID};
use crate::table::tuple::Data as TupleData;
use crate::writep;

#[derive(Debug, Clone, Copy)]
pub struct TableMeta {
    first_page_id: PageID,
    last_page_id: PageID,
}

impl Default for TableMeta {
    fn default() -> Self {
        Self { first_page_id: -1, last_page_id: -1 }
    }
}

pub type ListRef = Arc<List>;

pub struct List {
    pc: SharedPageCache,
    first_page_id: PageID,
    last_page_id: Mutex<PageID>,
}

impl List {
    pub fn new(
        pc: SharedPageCache,
        TableMeta { mut first_page_id, mut last_page_id }: TableMeta,
    ) -> crate::Result<ListRef> {
        assert!(
            (first_page_id == -1 && last_page_id == -1)
                || (first_page_id != -1 && last_page_id != -1)
        );

        if first_page_id == -1 || last_page_id == -1 {
            let page = pc.new_page()?;
            first_page_id = page.id;
            last_page_id = page.id;
        }

        Ok(Arc::new(Self { pc, first_page_id, last_page_id: Mutex::new(last_page_id) }))
    }

    pub fn default(pc: SharedPageCache) -> crate::Result<List> {
        let page = pc.new_page()?;
        let first_page_id = page.id;
        let last_page_id = page.id;
        drop(page);

        Ok(Self { pc, first_page_id, last_page_id: Mutex::new(last_page_id) })
    }

    fn last_page_id(&self) -> PageID {
        *self.last_page_id.lock().expect("todo")
    }

    fn last_page_id_mut(&self) -> std::sync::MutexGuard<'_, PageID> {
        self.last_page_id.lock().expect("todo")
    }

    pub fn iter(self: &Arc<Self>) -> Result<Iter> {
        let last_page_id = self.last_page_id();
        let page = self.pc.fetch_page(last_page_id)?;
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        Ok(Iter {
            list: Arc::clone(self),
            rid: RID { page_id: self.first_page_id, slot_id: 0 },
            end: RID { page_id: last_page_id, slot_id: node.len() },
        })
    }

    pub fn insert(&self, tuple_data: &TupleData) -> Result<Option<RID>> {
        self.insert_with_meta(tuple_data, &TupleMeta { deleted: false })
    }

    pub fn insert_with_meta(
        &self,
        tuple_data: &TupleData,
        meta: &TupleMeta,
    ) -> Result<Option<RID>> {
        let mut last_page_id = self.last_page_id_mut();
        let page = self.pc.fetch_page(*last_page_id)?;
        let mut page_w = page.write();
        let mut node = Node::from(&page_w.data);

        if let Some(slot_id) = node.insert(tuple_data, meta) {
            writep!(page_w, &PageBuf::from(&node));
            return Ok(Some(RID { page_id: *last_page_id, slot_id }));
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
                Ok(Some(RID { page_id: *last_page_id, slot_id }))
            }
            None => unreachable!(),
        }
    }

    pub fn get(&self, rid: RID) -> Result<Option<(TupleMeta, TupleData)>> {
        let page = self.pc.fetch_page(rid.page_id)?;
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        Ok(node.get(&rid))
    }

    pub fn update(&mut self, _meta: &TupleMeta) -> Result<()> {
        todo!()
    }
}

// Iter should hold a read lock and deserialised page?
pub struct Iter {
    list: ListRef,
    rid: RID,
    end: RID,
}

impl Iterator for Iter {
    type Item = Result<(TupleMeta, TupleData, RID)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end == self.rid {
            return None;
        }

        let result = match self.list.get(self.rid) {
            Ok(opt) => {
                let (meta, tuple) = opt?;
                Ok((meta, tuple, self.rid))
            }
            Err(e) => Err(e),
        };

        let page = match self.list.pc.fetch_page(self.rid.page_id) {
            Ok(p) => p,
            Err(e) => return Some(Err(e)),
        };
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        if self.rid.page_id == self.end.page_id && self.rid.slot_id == self.end.slot_id - 1 {
            // Last tuple, increment (so the next iteration returns None) and return result
            self.rid.slot_id += 1;
            return Some(result);
        } else if self.rid.slot_id + 1 < node.len() {
            self.rid.slot_id += 1;
        } else if node.next_page_id == 0 {
            return None;
        } else {
            self.rid = RID { page_id: node.next_page_id, slot_id: 0 }
        }

        Some(result)
    }
}

#[cfg(test)]
mod test {
    use {
        crate::{
            disk::Memory,
            page::PAGE_SIZE,
            page_cache::PageCache,
            replacer::LRU,
            table::list::List,
            table::{
                list::TableMeta,
                node::{TupleMeta, RID},
                tuple::Data as TupleData,
            },
        },
        bytes::BytesMut,
    };

    #[test]
    fn test_table() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let list = List::default(pc.clone())?;
        let want_a =
            TupleData(BytesMut::from(&std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8)[..]));
        let want_b =
            TupleData(BytesMut::from(&std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8)[..]));

        let rid_a = list.insert(&want_a)?.unwrap();
        let rid_b = list.insert(&want_b)?.unwrap();

        let list = List::new(
            pc,
            TableMeta { first_page_id: list.first_page_id, last_page_id: list.last_page_id() },
        )?;

        let (_, have_a) = list.get(rid_a)?.unwrap();
        let (_, have_b) = list.get(rid_b)?.unwrap();

        assert_eq!(want_a, have_a);
        assert_eq!(want_b, have_b);

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
        let list = List::new(pc.clone(), TableMeta { first_page_id, last_page_id: first_page_id })?;

        const WANT_LEN: usize = 100;
        let mut tuples = Vec::new();
        for i in 0..WANT_LEN {
            let tuple = TupleData(BytesMut::from(
                &std::array::from_fn::<u8, 150, _>(|j| (j * i) as u8)[..],
            ));
            list.insert(&tuple)?;
            tuples.push(tuple);
        }

        let have = list
            .iter()?
            .enumerate()
            .collect::<Vec<(usize, crate::Result<(TupleMeta, TupleData, RID)>)>>();

        assert_eq!(have.len(), WANT_LEN);

        for (i, result) in have {
            let (_, tuple, _) = result?;

            assert_eq!(tuples[i], tuple)
        }

        Ok(())
    }
}
