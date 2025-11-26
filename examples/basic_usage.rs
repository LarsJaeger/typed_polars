//! Basic usage example demonstrating typed DataFrames.

use typed_polars::prelude::*;

// Define a schema using the macro
typed_polars::define_schema! {
    UserSchema {
        id: i64,
        name: String,
        age: i32,
        salary: f64,
        active: bool,
    }
}

fn main() -> PolarsResult<()> {
    // Create a regular Polars DataFrame
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3, 4, 5]).into_column(),
        Series::new("name".into(), vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]).into_column(),
        Series::new("age".into(), vec![25i32, 30, 35, 28, 32]).into_column(),
        Series::new("salary".into(), vec![50000.0f64, 60000.0, 75000.0, 55000.0, 65000.0]).into_column(),
        Series::new("active".into(), vec![true, true, false, true, true]).into_column(),
    ])?;
    
    println!("Original DataFrame:");
    println!("{}", df);
    
    // Wrap it in a TypedDataFrame with schema validation
    let typed_df = TypedDataFrame::<UserSchema>::new(df)?;
    
    println!("\nTyped DataFrame created successfully!");
    println!("Shape: {:?}", typed_df.shape());
    
    // Type-safe column access using the schema module
    let name_series = typed_df.column(UserSchema::name)?;
    println!("\nName column:");
    println!("{:?}", name_series);
    
    let age_series = typed_df.column(UserSchema::age)?;
    println!("\nAge column:");
    println!("{:?}", age_series);
    
    // Sort by salary
    let sorted = typed_df.sort(UserSchema::salary, true)?;
    
    println!("\nSorted by salary (descending):");
    println!("{}", sorted);
    
    // Get head and tail
    let head = typed_df.head(Some(2));
    println!("\nFirst 2 rows:");
    println!("{}", head);
    
    let tail = typed_df.tail(Some(2));
    println!("\nLast 2 rows:");
    println!("{}", tail);
    
    Ok(())
}
