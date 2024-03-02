use bytes::BytesMut;

use crate::{
    disk::{Disk, FileSystem},
    page::{PageBuf, PageId},
    page_cache::{Result, SharedPageCache},
    table_page::{RId, Table, Tuple, TupleMeta},
    writep,
};

pub struct TableHeap<D: Disk = FileSystem> {
    pc: SharedPageCache<D>,
    first_page_id: PageId,
    last_page_id: PageId,
}

impl<D: Disk> TableHeap<D> {
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
        let table = Table::from(&page_r.data);

        Ok(Iter {
            heap: self,
            r_id: RId {
                page_id: self.first_page_id,
                slot_idx: 0,
            },
            end: RId {
                page_id: self.last_page_id,
                slot_idx: table.len(),
            },
        })
    }

    pub fn insert(&mut self, tuple_data: &BytesMut, meta: &TupleMeta) -> Result<Option<RId>> {
        let page = self.pc.fetch_page(self.last_page_id)?;
        let mut page_w = page.write();
        let mut table = Table::from(&page_w.data);

        if let Some(slot_idx) = table.insert(tuple_data, meta) {
            writep!(page_w, &PageBuf::from(&table));
            return Ok(Some(RId {
                page_id: self.last_page_id,
                slot_idx,
            }));
        }

        if table.len() == 0 {
            todo!("tuple too large error")
        }

        // Insert into a new page and set the next pointer
        let page = self.pc.new_page()?;
        let mut page_w = page.write();
        table.next_page_id = page.id;
        self.last_page_id = page.id;

        let mut table = Table::from(&page_w.data);
        match table.insert(tuple_data, meta) {
            Some(slot_idx) => {
                writep!(page_w, &PageBuf::from(&table));
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
        let table = Table::from(&page_r.data);

        let mut tuple = table.get(&r_id);
        if let Some((_, tuple)) = &mut tuple {
            tuple.r_id = r_id;
        }

        Ok(tuple)
    }
}

pub struct Iter<'a, D: Disk = FileSystem> {
    heap: &'a TableHeap<D>,
    r_id: RId,
    end: RId,
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use crate::{
        disk::Memory, page::PAGE_SIZE, page_cache::PageCache, replacer::LRU, table_heap::TableHeap,
        table_page::TupleMeta,
    };

    #[test]
    fn test_table() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 128;
        const K: usize = 2;

        let disk = Memory::new::<MEMORY>();
        let lru = LRU::new(K);
        let pc = PageCache::new(disk, lru, 0);

        let first_page_id = pc.new_page()?.id;
        let mut heap = TableHeap::new(pc.clone(), first_page_id, first_page_id);

        let meta = TupleMeta { deleted: false };
        let tuple_a = BytesMut::from(&std::array::from_fn::<u8, 10, _>(|i| (i * 2) as u8)[..]);
        let tuple_b = BytesMut::from(&std::array::from_fn::<u8, 15, _>(|i| (i * 3) as u8)[..]);

        let r_id_a = heap.insert(&tuple_a, &meta)?.unwrap();
        let r_id_b = heap.insert(&tuple_b, &meta)?.unwrap();

        let heap = TableHeap::new(pc, first_page_id, first_page_id);

        let (_, have_a) = heap.get(r_id_a)?.unwrap();
        let (_, have_b) = heap.get(r_id_b)?.unwrap();

        assert_eq!(tuple_a, have_a.data);
        assert_eq!(tuple_b, have_b.data);

        Ok(())
    }
}
