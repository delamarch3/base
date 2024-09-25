// select * from table_a;
// Project *
// Scan table_a

// select col_a, col_b from table_a where a = 'b';
// Project [col_b, col_b]
// Filter a = 'b'
// Scan table_a

// With index:
// Project [col_b, col_b]
// IndexScan table a = 'b'

// select col_a, col_b, table_b.col_a from table_a
// join table_b using (col_c)
// where table_a.col_a > 5;

// Project table_a.col_a, table_a.col_b, table_b.col_a
// HashJoin
// |---HashTableProbe table_a.col_a = table_b.col_a
// | HashTableBuild
// | Project col_a
// | Scan table_b
// Project col_a, col_b
// Filter col_a > 5
// Scan table_a

use crate::{catalog::Catalog, page_cache::PageCacheError, table::tuple::Tuple};

enum PlannerError {
    PageCacheError(PageCacheError),
}

type Result<T> = std::result::Result<T, PlannerError>;

struct Planner<'c> {
    catalog: &'c Catalog,
}

// TODO: misunderstanding of logical plan. The physical plan is what actually executes and is
// derived from the optimised logical plan. A logical plan is derived from the AST and describes
// how a query is executed at a high level. Build a quick and dumb logical plan from the AST and
// then use optimisations to transform it. This is the plan that is shown in EXPLAIN.
trait PlanNode {
    fn exec(&self, tuples: Vec<Tuple>) -> Result<Vec<Tuple>>;
}

struct Scan<'p, 'c> {
    planner: &'p Planner<'c>,
    table_name: String,
    //filters - does a logical plan need this?
    //index
}

impl<'p, 'c> PlanNode for Scan<'p, 'c> {
    fn exec(&self, mut tuples: Vec<Tuple>) -> Result<Vec<Tuple>> {
        let Some(info) = self.planner.catalog.get_table_by_name(&self.table_name) else {
            return Ok(tuples);
        };

        for result in info.table.iter().map_err(|e| PlannerError::PageCacheError(e))? {
            let (_, tuple) = result.map_err(|e| PlannerError::PageCacheError(e))?;
            tuples.push(tuple)
        }

        return Ok(tuples);
    }
}

struct Filter {
    nodes: Vec<Box<dyn PlanNode>>,
}

impl PlanNode for Filter {
    fn exec(&self, mut tuples: Vec<Tuple>) -> Result<Vec<Tuple>> {
        for node in &self.nodes {
            tuples = node.exec(tuples)?;
            // apply filter
        }

        Ok(tuples)
    }
}

struct PlanTree {
    nodes: Vec<Box<dyn PlanNode>>,
}
