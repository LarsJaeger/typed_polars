//! Example demonstrating CSV I/O with typed DataFrames.

use typed_polars::prelude::*;
use std::fs;

// Define a schema for sales data
typed_polars::define_schema! {
    SalesSchema {
        product_id: i64,
        product_name: String,
        quantity: i32,
        price: f64,
    }
}

fn main() -> PolarsResult<()> {
    // Create a sample CSV file
    let csv_content = "product_id,product_name,quantity,price
1,Laptop,5,999.99
2,Mouse,50,29.99
3,Keyboard,30,79.99
4,Monitor,10,299.99
5,Headphones,25,149.99";
    
    fs::write("sales_data.csv", csv_content).expect("Failed to write CSV");
    
    // Read the CSV with schema validation
    println!("Reading CSV file with typed schema...");
    let df = CsvReader::<SalesSchema>::new("sales_data.csv")
        .has_header(true)
        .finish()?;
    
    println!("Loaded DataFrame:");
    println!("{}", df);
    
    // Access columns in a type-safe way
    let product_names = df.column(SalesSchema::product_name)?;
    println!("\nProduct names:");
    println!("{:?}", product_names);
    
    // Write to a new CSV file
    println!("\nWriting to output.csv...");
    df.write_csv("output.csv")?;
    
    // Write to Parquet format
    println!("Writing to output.parquet...");
    df.write_parquet("output.parquet")?;
    
    // Read back from Parquet
    println!("\nReading from Parquet...");
    let df_from_parquet = ParquetReader::<SalesSchema>::new("output.parquet")
        .finish()?;
    
    println!("DataFrame from Parquet:");
    println!("{}", df_from_parquet);
    
    // Clean up
    fs::remove_file("sales_data.csv").ok();
    fs::remove_file("output.csv").ok();
    fs::remove_file("output.parquet").ok();
    
    println!("\nI/O operations completed successfully!");
    
    Ok(())
}
