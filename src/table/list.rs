use bytes::BytesMut;

use crate::{
    disk::{Disk, FileSystem},
    page::{PageBuf, PageId},
    page_cache::{Result, SharedPageCache},
    table::node::{Node, RId, Tuple, TupleMeta},
    writep,
};

pub struct List<D: Disk = FileSystem> {
    pc: SharedPageCache<D>,
    first_page_id: PageId,
    last_page_id: PageId,
}

impl<D: Disk> List<D> {
    pub fn new(pc: SharedPageCache<D>, first_page_id: PageId, last_page_id: PageId) -> Self {
        Self {
            pc,
            first_page_id,
            last_page_id,
        }
    }

    pub fn iter(&self) -> Result<Iter<'_, D>> {
        let page = self.pc.fetch_page(self.last_page_id)?;
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        Ok(Iter {
            list: self,
            r_id: RId {
                page_id: self.first_page_id,
                slot_idx: 0,
            },
            end: RId {
                page_id: self.last_page_id,
                slot_idx: node.len(),
            },
        })
    }

    pub fn insert(&mut self, tuple_data: &BytesMut, meta: &TupleMeta) -> Result<Option<RId>> {
        let page = self.pc.fetch_page(self.last_page_id)?;
        let mut page_w = page.write();
        let mut node = Node::from(&page_w.data);

        if let Some(slot_idx) = node.insert(tuple_data, meta) {
            writep!(page_w, &PageBuf::from(&node));
            return Ok(Some(RId {
                page_id: self.last_page_id,
                slot_idx,
            }));
        }

        if node.len() == 0 {
            todo!("tuple too large error")
        }

        // Insert into a new page and set the next pointer
        let page = self.pc.new_page()?;
        let mut page_w = page.write();
        node.next_page_id = page.id;
        self.last_page_id = page.id;

        let mut node = Node::from(&page_w.data);
        match node.insert(tuple_data, meta) {
            Some(slot_idx) => {
                writep!(page_w, &PageBuf::from(&node));
                Ok(Some(RId {
                    page_id: self.last_page_id,
                    slot_idx,
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
            tuple.r_id = r_id;
        }

        Ok(tuple)
    }
}

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
            Ok(opt) => opt.map(|t| Ok(t)),
            Err(e) => return Some(Err(e)),
        };

        let page = match self.list.pc.fetch_page(self.r_id.page_id) {
            Ok(p) => p,
            Err(e) => return Some(Err(e)),
        };
        let page_r = page.read();
        let node = Node::from(&page_r.data);

        if self.r_id.slot_idx + 1 < node.len() {
            self.r_id.slot_idx += 1;
        } else if node.next_page_id == 0 {
            return None;
        } else {
            self.r_id = RId {
                page_id: node.next_page_id,
                slot_idx: 0,
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use crate::{
        disk::Memory, page::PAGE_SIZE, page_cache::PageCache, replacer::LRU, table::list::List,
        table::node::TupleMeta,
    };

    #[test]
    fn test_table() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 1;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let first_page_id = pc.new_page()?.id;
        let mut list = List::new(pc.clone(), first_page_id, first_page_id);

        let meta = TupleMeta { deleted: false };
        let tuple_a = BytesMut::from(&std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8)[..]);
        let tuple_b = BytesMut::from(&std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8)[..]);

        let r_id_a = list.insert(&tuple_a, &meta)?.unwrap();
        let r_id_b = list.insert(&tuple_b, &meta)?.unwrap();

        let list = List::new(pc, first_page_id, first_page_id);

        let (_, have_a) = list.get(r_id_a)?.unwrap();
        let (_, have_b) = list.get(r_id_b)?.unwrap();

        assert_eq!(tuple_a, have_a.data);
        assert_eq!(tuple_b, have_b.data);

        Ok(())
    }

    #[test]
    fn test_iter() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 1;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let first_page_id = pc.new_page()?.id;
        let mut list = List::new(pc.clone(), first_page_id, first_page_id);

        let meta = TupleMeta { deleted: false };
        let mut tuples = Vec::new();
        for i in 0..10 {
            let tuple = BytesMut::from(&std::array::from_fn::<u8, 10, _>(|j| (j * i) as u8)[..]);
            list.insert(&tuple, &meta)?;
            tuples.push(tuple);
        }

        for (i, result) in list.iter()?.enumerate() {
            let (_, tuple) = result?;

            eprintln!("{:?}", tuple);
            assert_eq!(tuples[i], tuple.data)
        }

        Ok(())
    }
}
