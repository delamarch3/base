//!

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU32, Ordering::Relaxed},
};

use crate::{
    disk::{Disk, FileSystem},
    page_cache::SharedPageCache,
    table::list::List as Table,
};

pub enum ColumnType {
    Int,
    Varchar,
}

pub enum Length {
    Fixed(u8),
    Variable,
}

pub struct Column {
    name: String,
    ty: ColumnType,
    len: Length,
    offset: u32,
}

pub struct Schema {
    columns: Vec<Column>,
}

pub type OId = u32;

pub struct TableInfo<D: Disk = FileSystem> {
    name: String,
    schema: Schema,
    oid: OId,
    table: Table<D>,
}

pub struct IndexMeta {
    name: String,
    table_name: String,
    column_ids: Vec<u32>,
    schema: Schema,
}

pub enum IndexType {
    HashTable,
    BTree,
}

pub struct IndexInfo {
    name: String,
    schema: Schema,
    oid: OId,
    index_ty: IndexType,
}

pub struct Catalog<D: Disk = FileSystem> {
    pc: SharedPageCache<D>,
    tables: HashMap<OId, TableInfo<D>>,
    table_names: HashMap<String, OId>,
    next_table_oid: AtomicU32,
    indexes: HashMap<OId, IndexInfo>,
    index_names: HashMap<String, HashMap<String, OId>>,
    next_index_oid: AtomicU32,
}

impl<D: Disk> Catalog<D> {
    pub fn new(pc: SharedPageCache<D>) -> Self {
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

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Option<&TableInfo<D>> {
        if self.table_names.contains_key(name) {
            return None;
        }

        let oid = self.next_index_oid.fetch_add(1, Relaxed);
        let info = TableInfo {
            name: name.into(),
            schema,
            oid,
            table: Table::default(self.pc.clone()),
        };

        self.table_names.insert(name.into(), oid);
        self.index_names.insert(name.into(), HashMap::new());
        self.tables.insert(oid, info);

        self.tables.get(&oid)
    }

    pub fn get_table_by_oid(&self, oid: OId) -> Option<&TableInfo<D>> {
        self.tables.get(&oid)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableInfo<D>> {
        let oid = self.table_names.get(name)?;
        self.tables.get(oid)
    }

    pub fn list_tables(&self) -> Vec<&String> {
        self.table_names.keys().collect()
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        schema: &Schema,
        column_ids: &Vec<u32>,
    ) -> Option<IndexInfo> {
        todo!()
    }

    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<IndexInfo> {
        todo!()
    }

    pub fn get_index_by_oid(&self, oid: OId) -> Option<IndexInfo> {
        todo!()
    }

    pub fn list_indexes(&self) -> Vec<IndexInfo> {
        todo!()
    }
}
