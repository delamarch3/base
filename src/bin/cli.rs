use std::{
    io::{stdin, stdout, Write},
    sync::{Arc, Mutex},
};

use base::{
    catalog::Catalog, disk::FileSystem, execution::execute, logical_plan::LogicalOperator,
    optimiser::Optimiser, page_cache::PageCache, physical_plan::PhysicalOperator, planner::Planner,
    replacer::LRU, sql::Parser,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> Result<()> {
    let disk = FileSystem::new("db.base")?;
    let replacer = LRU::new(2);
    let pc = PageCache::new(disk, replacer, 0);
    let catalog = Arc::new(Mutex::new(Catalog::new(pc)));
    let planner = Planner::new(Arc::clone(&catalog));
    let optimiser = Optimiser::new(Arc::clone(&catalog));

    let stdin = stdin();
    let mut stdout = stdout();
    loop {
        stdout.write_all(b"(base) ")?;
        stdout.flush()?;

        let mut input = String::new();
        stdin.read_line(&mut input)?;

        if let Err(e) = run_query(&input, &planner, &optimiser) {
            writeln!(stdout, "{e}")?;
        };
    }
}

fn run_query(input: &str, planner: &Planner, optimiser: &Optimiser) -> Result<()> {
    let mut stdout = stdout();

    let mut parser = Parser::new(input)?;

    let plans = parser
        .parse_statements()?
        .into_iter()
        .map(|stmt| {
            planner.plan_statement(stmt).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        })
        .collect::<Result<Vec<LogicalOperator>>>()?;

    let physical_plans = plans
        .into_iter()
        .map(|p| optimiser.transform(p))
        .map(|p| optimiser.implement(p))
        .collect::<Result<Vec<Box<dyn PhysicalOperator>>>>()?;

    for mut plan in physical_plans {
        let result = execute(plan.as_mut())?;
        let schema = plan.schema();

        for row in &result {
            for column in &schema.columns {
                let value = row.get_value(column.offset, column.ty);
                write!(stdout, "{value} ")?;
            }
            writeln!(stdout)?;
        }
    }

    Ok(())
}
