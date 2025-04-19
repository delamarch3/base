pub mod schema;
use schema::Schema;

use crate::btree::BTree;
use crate::page::PageID;
use crate::page_cache::SharedPageCache;
use crate::table::{
    list::{List as TableInner, ListRef as TableRef},
    node::RID,
    tuple::{fit_tuple_with_schema, Data as TupleData},
};

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering::Relaxed},
    Arc,
};

pub type OID = u32;

pub struct TableInfo {
    pub name: String,
    pub schema: Schema,
    pub oid: OID,
    pub table: TableRef,
}

impl Clone for TableInfo {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            schema: self.schema.clone(),
            oid: self.oid,
            table: self.table.clone(),
        }
    }
}

pub struct IndexMeta {
    pub name: String,
    pub table_name: String,
    pub column_ids: Vec<u32>,
    pub schema: Schema,
}

#[derive(Debug)]
pub enum IndexType {
    HashTable,
    BTree,
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::HashTable => write!(f, "HashTable"),
            IndexType::BTree => write!(f, "BTree"),
        }
    }
}

#[derive(Debug)]
pub struct IndexInfo {
    pub name: String,
    pub schema: Schema,
    pub oid: OID,
    pub index_ty: IndexType,
    pub root_page_id: PageID,
}

pub struct Catalog {
    pc: SharedPageCache,
    tables: HashMap<OID, Arc<TableInfo>>,
    table_names: HashMap<String, OID>,
    next_table_oid: AtomicU32,
    indexes: HashMap<OID, Arc<IndexInfo>>,
    index_names: HashMap<String, HashMap<String, OID>>, // table -> index -> oid
    next_index_oid: AtomicU32,
}

impl Catalog {
    pub fn new(pc: SharedPageCache) -> Self {
        Self {
            pc,
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_oid: AtomicU32::new(0),
            indexes: HashMap::new(),
            index_names: HashMap::new(),
            next_index_oid: AtomicU32::new(0),
        }
    }

    pub fn create_table(
        &mut self,
        name: &str,
        schema: impl Into<Schema>,
    ) -> crate::Result<Option<Arc<TableInfo>>> {
        if self.table_names.contains_key(name) {
            return Ok(None);
        }

        let oid = self.next_table_oid.fetch_add(1, Relaxed);
        let info = TableInfo {
            name: name.into(),
            schema: schema.into(),
            oid,
            table: Arc::new(TableInner::default(self.pc.clone())?),
        };

        self.table_names.insert(name.into(), oid);
        self.index_names.insert(name.into(), HashMap::new());
        self.tables.insert(oid, Arc::new(info));

        Ok(self.tables.get(&oid).map(|info| info.clone()))
    }

    pub fn get_table_by_oid(&self, oid: OID) -> Option<Arc<TableInfo>> {
        self.tables.get(&oid).map(|info| info.clone())
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.tables.get(self.table_names.get(name)?).map(|info| info.clone())
    }

    pub fn list_tables(&self) -> Vec<&String> {
        self.table_names.keys().collect()
    }

    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        index_ty: IndexType,
        schema: &Schema,
        key: &[&str],
    ) -> Option<Arc<IndexInfo>> {
        // TODO: verify key schema against table schema

        if self.index_names.contains_key(index_name) {
            return None;
        }

        let indexed_table = self.index_names.get_mut(table_name)?;
        if indexed_table.contains_key(index_name) {
            // Index with name already exists
            return None;
        }

        // Schema for creating key tuple from table tuple (offsets could be sparse)
        let tuple_schema = schema.filter(key);

        // Correct offsets for the index so they are read/written correctly
        let index_schema = tuple_schema.compact();

        let root_page_id = match index_ty {
            IndexType::HashTable => todo!(),
            IndexType::BTree => {
                let mut btree = BTree::<RID>::new(self.pc.clone(), &index_schema);
                let info = self.tables.get(&self.table_names[table_name])?;
                for result in info.table.iter().expect("todo") {
                    // Remove columns from the tuple to match schema
                    let (_, tuple, rid) = result.expect("todo");
                    let tuple = fit_tuple_with_schema(&tuple, &tuple_schema);
                    btree.insert(&tuple, &rid).expect("todo");
                }

                btree.root()
            }
        };

        let oid = self.next_index_oid.fetch_add(1, Relaxed);
        indexed_table.insert(index_name.into(), oid);

        let info = IndexInfo {
            name: index_name.into(),
            schema: index_schema,
            oid,
            index_ty,
            root_page_id,
        };

        self.indexes.insert(oid, Arc::new(info));
        indexed_table.insert(index_name.into(), oid);

        self.indexes.get(&oid).map(|info| info.clone())
    }

    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<Arc<IndexInfo>> {
        self.indexes
            .get(self.index_names.get(table_name)?.get(index_name)?)
            .map(|info| info.clone())
    }

    pub fn get_index_by_oid(&self, oid: OID) -> Option<Arc<IndexInfo>> {
        self.indexes.get(&oid).map(|info| info.clone())
    }

    pub fn list_indexes(&self) -> Vec<Arc<IndexInfo>> {
        self.indexes.values().map(|info| info.clone()).collect()
    }
}

#[cfg(test)]
mod test {
    use crate::btree::BTree;
    use crate::catalog::{Catalog, IndexType};
    use crate::disk::Memory;
    use crate::page::PAGE_SIZE;
    use crate::page_cache::PageCache;
    use crate::replacer::LRU;
    use crate::table::{node::RID, tuple::Builder as TupleBuilder};
    use crate::{column, schema};

    macro_rules! test_btree_index {
        ($test:tt, $schema:expr, $key:expr, $tuples:expr, $want:expr) => {
            #[test]
            fn $test() -> crate::Result<()> {
                const MEMORY: usize = PAGE_SIZE * 8;
                const K: usize = 2;
                let memory = Memory::new::<MEMORY>();
                let replacer = LRU::new(K);
                let pc = PageCache::new(memory, replacer, 0);

                let schema = $schema;

                const TABLE_A: &str = "table_a";
                const INDEX_A: &str = "index_a";
                let mut catalog = Catalog::new(pc.clone());
                catalog.create_table(TABLE_A, schema.clone())?;
                let info = catalog.get_table_by_name(TABLE_A).expect("table_a should exist");

                for tuple in $tuples {
                    info.table.insert(&tuple)?.expect("there should be a rid");
                }

                let index_schema = schema.filter(&$key).compact();

                catalog.create_index(INDEX_A, TABLE_A, IndexType::BTree, &schema, &$key);
                let index = catalog.get_index(TABLE_A, INDEX_A).expect("index_a should exist");
                let index: BTree<RID> =
                    BTree::new_with_root(pc.clone(), index.root_page_id, &index_schema);
                let have = index.scan()?;

                assert_eq!($want, have);

                Ok(())
            }
        };
    }

    test_btree_index!(
        test_int_big_int_key,
        schema! { column!("c1", Int), column!("c2", Varchar), column!("c3", BigInt) },
        ["c1", "c3"],
        [
            TupleBuilder::new().int(10).varchar("row_a").big_int(20).build(),
            TupleBuilder::new().int(20).varchar("row_b").big_int(30).build()
        ],
        vec![
            (TupleBuilder::new().int(10).big_int(20).build(), RID { page_id: 0, slot_id: 0 },),
            (TupleBuilder::new().int(20).big_int(30).build(), RID { page_id: 0, slot_id: 1 },),
        ]
    );

    test_btree_index!(
        test_int_varchar_key,
        schema! { column!("c1", Int), column!("c2", BigInt), column!("c3", Varchar) },
        ["c1", "c3"],
        [
            TupleBuilder::new().int(20).big_int(20).varchar("row_a").build(),
            TupleBuilder::new().int(20).big_int(30).varchar("row_b").build()
        ],
        vec![
            (TupleBuilder::new().int(20).varchar("row_a").build(), RID { page_id: 0, slot_id: 0 },),
            (TupleBuilder::new().int(20).varchar("row_b").build(), RID { page_id: 0, slot_id: 1 },),
        ]
    );
}
