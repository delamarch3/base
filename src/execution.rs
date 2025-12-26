use crate::{physical_plan::PhysicalOperator, table::tuple::Data as TupleData};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn execute(plan: &mut dyn PhysicalOperator) -> Result<Vec<TupleData>> {
    let mut result = Vec::new();
    while let Some(data) = plan.next()? {
        result.push(data);
    }

    Ok(result)
}
