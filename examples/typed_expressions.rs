//! Example demonstrating type-safe expressions with lazy evaluation.

use typed_polars::prelude::*;
use typed_polars::expr::col;

// Define a schema for employee data
typed_polars::define_schema! {
    EmployeeSchema {
        employee_id: i64,
        name: String,
        department: String,
        salary: f64,
        years_of_service: i32,
    }
}

fn main() -> PolarsResult<()> {
    // Create a sample DataFrame
    let df = DataFrame::new(vec![
        Series::new("employee_id".into(), vec![1i64, 2, 3, 4, 5]).into_column(),
        Series::new("name".into(), vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]).into_column(),
        Series::new("department".into(), vec!["Engineering", "Sales", "Engineering", "HR", "Sales"]).into_column(),
        Series::new("salary".into(), vec![75000.0f64, 55000.0, 80000.0, 60000.0, 58000.0]).into_column(),
        Series::new("years_of_service".into(), vec![5i32, 3, 7, 4, 2]).into_column(),
    ])?;
    
    let typed_df = TypedDataFrame::<EmployeeSchema>::new(df)?;
    
    println!("Original DataFrame:");
    println!("{}\n", typed_df);
    
    // Use lazy evaluation with type-safe expressions
    let lazy_df = typed_df.lazy();
    
    // Type-safe column reference
    let dept_expr = col(EmployeeSchema::department);
    let salary_expr = col(EmployeeSchema::salary);
    
    // Filter for Engineering department
    let engineering = lazy_df
        .clone()
        .filter(dept_expr.clone().into_inner().eq(polars::prelude::lit("Engineering")))
        .collect()?;
    
    println!("Engineering Department:");
    println!("{}\n", engineering);
    
    // Calculate statistics
    let stats = lazy_df
        .clone()
        .group_by([EmployeeSchema::department.name()])
        .agg([
            salary_expr.clone().mean().alias("avg_salary").into_inner(),
            salary_expr.clone().min().alias("min_salary").into_inner(),
            salary_expr.max().alias("max_salary").into_inner(),
        ])
        .collect()?;
    
    println!("Salary Statistics by Department:");
    println!("{}", stats);
    
    Ok(())
}
