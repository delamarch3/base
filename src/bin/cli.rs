use std::{
    cmp::max,
    io::{stdin, stdout, Write},
    sync::{Arc, Mutex},
};

use base::{
    catalog::Catalog, disk::FileSystem, execution::execute, optimiser::Optimiser,
    page_cache::PageCache, physical_plan::PhysicalOperator, planner::Planner, replacer::LRU,
    sql::Parser,
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

    let mut input = String::new();
    loop {
        stdout.write_all(b"(base) ")?;
        stdout.flush()?;

        input.clear();
        stdin.read_line(&mut input)?;
        if input.is_empty() {
            writeln!(stdout)?;
            continue;
        }

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
            planner
                .plan(stmt)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                .map(|p| optimiser.transform(p))
                .map(|p| optimiser.implement(p))
        })
        .collect::<Result<Vec<Box<dyn PhysicalOperator>>>>()?;

    for mut plan in plans {
        let result = execute(plan.as_mut())?;
        let schema = plan.schema();

        let mut column_widths = vec![0; schema.len()];
        for (i, column) in schema.iter().enumerate() {
            column_widths[i] = max(column_widths[i], column.name.len() + 2); // padding = 2
        }

        // TODO: look at the some of the rows to figure out the width

        for i in 0..schema.len() {
            let width = column_widths[i];
            write!(stdout, "+{:-^width$}", "")?;
        }
        writeln!(stdout, "+")?;

        for (i, column) in schema.iter().enumerate() {
            let width = column_widths[i];
            let name = &column.name;
            write!(stdout, "|{name:^width$}")?;
        }
        writeln!(stdout, "|")?;

        for row in &result {
            for i in 0..schema.len() {
                let width = column_widths[i];
                write!(stdout, "+{:-^width$}", "")?;
            }
            writeln!(stdout, "+")?;

            for (i, column) in schema.iter().enumerate() {
                let value = row.get_value(column.offset, column.ty);
                let width = column_widths[i];
                write!(stdout, "|{value:^width$}")?;
            }
            writeln!(stdout, "|")?;
        }

        for i in 0..schema.len() {
            let width = column_widths[i];
            write!(stdout, "+{:-^width$}", "")?;
        }
        writeln!(stdout, "+")?;
    }

    Ok(())
}
