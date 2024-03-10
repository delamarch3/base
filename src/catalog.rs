//!

use std::{
    collections::HashMap,
    mem::size_of,
    sync::atomic::{AtomicU32, Ordering::Relaxed},
};

use crate::{
    btree::BTree,
    disk::{Disk, FileSystem},
    page_cache::SharedPageCache,
    table::{
        list::List as Table,
        tuple::{RId, Tuple},
    },
};

// TODO: Move to tuple?
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    TinyInt(i8),
    Bool(bool),
    Int(i32),
    BigInt(i64),
    Varchar(String),
}

impl Value {
    pub fn from(column: &Column, data: &[u8]) -> Value {
        let data = match column.ty {
            Type::Varchar => {
                // First two bytes is the offset
                let offset =
                    u16::from_be_bytes(data[column.offset..column.offset + 2].try_into().unwrap())
                        as usize;

                // Second two bytes is the size
                let size = u16::from_be_bytes(
                    data[column.offset + 2..column.offset + 4]
                        .try_into()
                        .unwrap(),
                ) as usize;

                assert!(offset + size <= data.len());

                &data[offset..offset + size]
            }
            _ => {
                assert!(column.offset + column.size() <= data.len());
                &data[column.offset..column.offset + column.size()]
            }
        };

        match column.ty {
            Type::TinyInt => {
                assert_eq!(data.len(), size_of::<i8>());
                Value::TinyInt(i8::from_be_bytes(data.try_into().unwrap()))
            }
            Type::Bool => {
                assert_eq!(data.len(), size_of::<bool>());
                Value::Bool(u8::from_be_bytes(data.try_into().unwrap()) > 0)
            }
            Type::Int => {
                assert_eq!(data.len(), size_of::<i32>());
                Value::Int(i32::from_be_bytes(data.try_into().unwrap()))
            }
            Type::BigInt => {
                assert_eq!(data.len(), size_of::<i64>());
                Value::BigInt(i64::from_be_bytes(data.try_into().unwrap()))
            }
            Type::Varchar => {
                let str = std::str::from_utf8(data).expect("todo");
                Value::Varchar(str.into())
            }
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::TinyInt(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Int(v) => write!(f, "{}", v),
            Value::BigInt(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug)]
pub enum Type {
    TinyInt,
    Bool,
    Int,
    BigInt,
    Varchar,
}

impl Type {
    pub fn size(&self) -> usize {
        match self {
            Type::TinyInt | Type::Bool => 1,
            Type::Int => 4,
            Type::BigInt => 8,
            Type::Varchar => 4, // [offset(2) , size(2)]
        }
    }
}

pub enum Length {
    Fixed(u8),
    Variable,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub offset: usize,
}

impl Column {
    pub fn size(&self) -> usize {
        self.ty.size()
    }
}

pub struct Schema {
    columns: Vec<Column>,
    size: usize,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            size: columns.iter().fold(0, |acc, c| acc + c.size()),
            columns,
        }
    }
}

impl Schema {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Column> {
        self.columns.iter()
    }
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
        &mut self,
        index_name: &str,
        table_name: &str,
        index_ty: IndexType,
        schema: Schema,
        column_ids: &Vec<u32>,
    ) -> Option<IndexInfo> {
        if self.index_names.contains_key(index_name) {
            return None;
        }

        let indexed_table = self.index_names.get_mut(table_name)?;
        if indexed_table.contains_key(index_name) {
            // Index with name already exists
            return None;
        }

        match index_ty {
            IndexType::HashTable => todo!(),
            IndexType::BTree => {
                // TODO: Use key schema
                let mut btree = BTree::<Tuple, RId, _>::new(self.pc.clone(), 16);
                let info = self.tables.get(&self.table_names[table_name])?;
                for result in info.table.iter().expect("todo") {
                    let (_, tuple) = result.expect("todo");
                    btree.insert(&tuple, &tuple.rid).expect("todo");
                }

                // TODO: Save this somewhere
                let _root = btree.root();
            }
        };

        let oid = self.next_index_oid.fetch_add(1, Relaxed);
        indexed_table.insert(index_name.into(), oid);

        let info = IndexInfo {
            name: index_name.into(),
            schema,
            oid,
            index_ty,
        };
        self.indexes.insert(oid, info);

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
