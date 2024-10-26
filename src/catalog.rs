use {
    crate::{
        btree::BTree,
        disk::{Disk, FileSystem},
        page::PageID,
        page_cache::SharedPageCache,
        table::{
            list::{List as TableInner, SharedList as Table},
            node::RID,
            tuple::{fit_tuple_with_schema, Data as TupleData},
        },
    },
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU32, Ordering::Relaxed},
            Arc,
        },
    },
};

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum Type {
    TinyInt,
    Bool,
    Int,
    BigInt,
    Varchar,
}

impl Type {
    /// Returns the size of any value of the type at tuple level
    /// Since varchar is variable length, we only store the offset and
    /// the size at the tuple level (2 bytes each)
    pub fn size(&self) -> usize {
        match self {
            Type::TinyInt | Type::Bool => 1,
            Type::Int => 4,
            Type::BigInt => 8,
            Type::Varchar => 4,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: Type,
    pub offset: usize,
}

impl Column {
    pub fn value_size(&self) -> usize {
        self.ty.size()
    }
}

impl<const N: usize> From<[(&str, Type); N]> for Schema {
    fn from(value: [(&str, Type); N]) -> Self {
        let mut columns = Vec::new();

        let mut offset = 0;
        for (name, ty) in value {
            columns.push(Column { name: name.into(), ty, offset });
            offset += ty.size();
        }

        Self { tuple_size: offset, columns }
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
pub struct Schema {
    pub columns: Vec<Column>,
    tuple_size: usize,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { tuple_size: columns.iter().fold(0, |acc, c| acc + c.value_size()), columns }
    }

    /// Returns a new `Schema` which has been filtered to include the specified columns
    pub fn filter(&self, columns: &[&str]) -> Self {
        let mut tuple_size = 0;
        let columns = self
            .iter()
            .filter(|Column { name, .. }| columns.contains(&name.as_str()))
            .map(|col| {
                tuple_size += col.ty.size();
                col.clone()
            })
            .collect();

        Self { columns, tuple_size }
    }

    /// Returns a new `Schema` where the offsets have been adjusted so that each column is packed
    /// together
    pub fn compact(&self) -> Self {
        let mut schema = self.clone();
        let mut current = 0;

        for Column { ty, offset, .. } in &mut schema.columns {
            *offset = current;
            current += ty.size();
        }

        schema
    }

    /// Returns a new `Schema` where another `Schema` is appended
    pub fn join(&self, other: &Schema) -> Self {
        let mut schema = self.clone();
        schema.columns.extend(other.columns.iter().map(|column| column.clone()));
        schema.tuple_size += other.tuple_size;

        schema
    }

    pub fn tuple_size(&self) -> usize {
        self.tuple_size
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Column> {
        self.columns.iter()
    }
}

pub type OID = u32;

pub struct TableInfo<D: Disk = FileSystem> {
    pub name: String,
    pub schema: Schema,
    pub oid: OID,
    pub table: Table<D>,
}

impl<D> Clone for TableInfo<D>
where
    D: Disk,
{
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
    name: String,
    table_name: String,
    column_ids: Vec<u32>,
    schema: Schema,
}

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

pub struct IndexInfo {
    pub name: String,
    pub schema: Schema,
    pub oid: OID,
    pub index_ty: IndexType,
    pub root_page_id: PageID,
}

pub struct Catalog<D: Disk = FileSystem> {
    pc: SharedPageCache<D>,
    // TODO: considering putting TableInfo (and IndexInfo) inside Arc
    tables: HashMap<OID, TableInfo<D>>,
    table_names: HashMap<String, OID>,
    next_table_oid: AtomicU32,
    indexes: HashMap<OID, IndexInfo>,
    index_names: HashMap<String, HashMap<String, OID>>, // table -> index -> oid
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

    pub fn create_table(
        &mut self,
        name: &str,
        schema: impl Into<Schema>,
    ) -> crate::Result<Option<&TableInfo<D>>> {
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
        self.tables.insert(oid, info);

        Ok(self.tables.get(&oid))
    }

    pub fn get_table_by_oid(&self, oid: OID) -> Option<&TableInfo<D>> {
        self.tables.get(&oid)
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<&TableInfo<D>> {
        self.tables.get(self.table_names.get(name)?)
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
    ) -> Option<&IndexInfo> {
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

        let root;
        match index_ty {
            IndexType::HashTable => todo!(),
            IndexType::BTree => {
                let mut btree = BTree::<RID, _>::new(self.pc.clone(), &index_schema);
                let info = self.tables.get(&self.table_names[table_name])?;
                for result in info.table.iter().expect("todo") {
                    // Remove columns from the tuple to match schema
                    let (_, TupleData(data), rid) = result.expect("todo");
                    let tuple = fit_tuple_with_schema(&data, &tuple_schema);
                    btree.insert(&tuple, &rid).expect("todo");
                }

                root = btree.root();
            }
        };

        let oid = self.next_index_oid.fetch_add(1, Relaxed);
        indexed_table.insert(index_name.into(), oid);

        self.indexes.insert(
            oid,
            IndexInfo {
                name: index_name.into(),
                schema: index_schema,
                oid,
                index_ty,
                root_page_id: root,
            },
        );
        indexed_table.insert(index_name.into(), oid);

        self.indexes.get(&oid)
    }

    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<&IndexInfo> {
        self.indexes.get(self.index_names.get(table_name)?.get(index_name)?)
    }

    pub fn get_index_by_oid(&self, oid: OID) -> Option<&IndexInfo> {
        self.indexes.get(&oid)
    }

    pub fn list_indexes(&self) -> Vec<&IndexInfo> {
        self.indexes.iter().map(|(_, info)| info).collect()
    }
}

#[cfg(test)]
mod test {
    use crate::{
        btree::BTree,
        catalog::{Catalog, IndexType, Schema, Type},
        disk::Memory,
        page::PAGE_SIZE,
        page_cache::PageCache,
        replacer::LRU,
        table::{
            node::{TupleMeta, RID},
            tuple::{Builder as TupleBuilder, Data as TupleData},
        },
    };

    #[test]
    fn test_btree_index() -> crate::Result<()> {
        const MEMORY: usize = PAGE_SIZE * 16;
        const K: usize = 2;
        let memory = Memory::new::<MEMORY>();
        let replacer = LRU::new(K);
        let pc = PageCache::new(memory, replacer, 0);

        struct Test {
            schema: Schema,
            key: &'static [&'static str],
            tuples: Vec<TupleData>,
            want: Vec<(TupleData, RID)>,
        }

        let tcs = [
            Test {
                schema: [("col_a", Type::Int), ("col_b", Type::Varchar), ("col_c", Type::BigInt)]
                    .into(),
                key: &["col_a", "col_c"],
                tuples: vec![
                    TupleBuilder::new()
                        .int(10)
                        .varchar("row_a") // TODO: slot panics when this is the last column?
                        .big_int(20)
                        .build(),
                    TupleBuilder::new()
                        .int(20)
                        .varchar("row_b") // TODO: slot panics when this is the last column?
                        .big_int(30)
                        .build(),
                ],
                want: vec![
                    (
                        TupleBuilder::new().int(10).big_int(20).build(),
                        RID { page_id: 0, slot_id: 0 },
                    ),
                    (
                        TupleBuilder::new().int(20).big_int(30).build(),
                        RID { page_id: 0, slot_id: 1 },
                    ),
                ],
            },
            Test {
                schema: [("col_a", Type::Int), ("col_b", Type::BigInt), ("col_c", Type::Varchar)]
                    .into(),
                key: &["col_a", "col_c"],
                tuples: vec![
                    TupleBuilder::new().int(20).big_int(20).varchar("row_a").build(),
                    TupleBuilder::new().int(20).big_int(30).varchar("row_b").build(),
                ],
                want: vec![
                    (
                        TupleBuilder::new().int(20).varchar("row_a").build(),
                        RID { page_id: 2, slot_id: 0 },
                    ),
                    (
                        TupleBuilder::new().int(20).varchar("row_b").build(),
                        RID { page_id: 2, slot_id: 1 },
                    ),
                ],
            },
        ];

        const TABLE_A: &str = "table_a";
        const INDEX_A: &str = "index_a";
        for Test { schema, key, tuples, want } in tcs {
            let mut catalog = Catalog::new(pc.clone());
            catalog.create_table(TABLE_A, schema.clone())?;
            let info = catalog.get_table_by_name(TABLE_A).expect("table_a should exist");

            for tuple in tuples {
                info.table
                    .insert(&tuple, &TupleMeta { deleted: false })?
                    .expect("there should be a rid");
            }

            let index_schema = schema.filter(key).compact();

            catalog.create_index(INDEX_A, TABLE_A, IndexType::BTree, &schema, &["col_a", "col_c"]);
            let index = catalog.get_index(TABLE_A, INDEX_A).expect("index_a should exist");
            let index: BTree<RID, _> =
                BTree::new_with_root(pc.clone(), index.root_page_id, &index_schema);
            let have = index.scan()?;

            assert_eq!(want, have);
        }

        Ok(())
    }
}
