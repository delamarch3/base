use crate::catalog::Schema;

/// The first value will always be Some(..) unless it's a Scan. Binary operators like joins should
/// have both
pub type LogicalPlanInputs<'a> =
    (Option<&'a Box<dyn LogicalPlan>>, Option<&'a Box<dyn LogicalPlan>>);

pub trait LogicalPlan: std::fmt::Display {
    fn schema(&self) -> &Schema;
    fn inputs(&self) -> LogicalPlanInputs;
}

pub fn format_logical_plan(plan: &Box<dyn LogicalPlan>) -> String {
    fn format_logical_plan(plan: &Box<dyn LogicalPlan>, indent: u16) -> String {
        let mut output = String::new();
        (0..indent).for_each(|_| output.push('\t'));
        output.push_str(&plan.to_string());
        output.push('\n');

        let (lhs, rhs) = plan.inputs();
        if let Some(plan) = lhs {
            output.push_str(&format_logical_plan(&plan, indent + 1));
        }
        if let Some(plan) = rhs {
            output.push_str(&format_logical_plan(&plan, indent + 1));
        }

        output
    }

    format_logical_plan(plan, 0)
}

// TODO
pub enum Expr {
    IsNull(String),
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TODO")?;

        Ok(())
    }
}

pub struct Scan {
    table: String,
    schema: Schema,
}

impl std::fmt::Display for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scan: table={}, projection=", self.table)?;
        for column in self.schema.columns() {
            write!(f, "{},", column.name)?;
        }

        Ok(())
    }
}

impl LogicalPlan for Scan {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (None, None)
    }
}

impl Scan {
    pub fn new(table: String, schema: Schema) -> Self {
        Self { table, schema }
    }
}

pub struct Filter {
    expr: Expr,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter: expr={}", self.expr)?;

        Ok(())
    }
}

impl LogicalPlan for Filter {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Filter {
    pub fn new(expr: Expr, input: Box<dyn LogicalPlan>) -> Self {
        Self { expr, input }
    }
}

// TODO: keeping this simple for now but should be able to support expressions too
pub struct Projection {
    schema: Schema,
    input: Box<dyn LogicalPlan>,
}

impl std::fmt::Display for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Projection: ")?;
        for column in self.schema.columns() {
            write!(f, "{},", column.name)?;
        }

        Ok(())
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn inputs(&self) -> LogicalPlanInputs {
        (Some(&self.input), None)
    }
}

impl Projection {
    pub fn new(exprs: &[&str], input: Box<dyn LogicalPlan>) -> Self {
        let schema = input.schema().filter(exprs);
        Self { schema, input }
    }
}

#[cfg(test)]
mod test {
    use {
        super::{format_logical_plan, Expr, Filter, LogicalPlan, Projection, Scan},
        crate::catalog::Type,
    };

    #[test]
    fn test_format_logical_plan() {
        let schema =
            [("col_a", Type::Int), ("col_b", Type::Varchar), ("col_c", Type::BigInt)].into();
        let scan = Scan::new("t1".into(), schema);
        let filter = Filter::new(Expr::IsNull("col_a".into()), Box::new(scan));
        let projection = Projection::new(&["col_a", "col_b"], Box::new(filter));

        let have = format_logical_plan(&(Box::new(projection) as Box<dyn LogicalPlan>));
        let want = "\
Projection: col_a,col_b,
	Filter: expr=TODO
		Scan: table=t1, projection=col_a,col_b,col_c,
";

        assert_eq!(want, have)
    }
}
