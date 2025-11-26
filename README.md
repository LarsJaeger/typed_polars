# typed_polars

> Disclaimer: This project is experimental and 100% vibe coded (1 prompt, lol). Validation is in progress, but I would not trust it at the moment! If there exist similar frameworks, please let me know, I could not find one.

A statically-typed wrapper around [Polars](https://pola.rs/) that provides compile-time type safety for DataFrame schemas and operations.

## Features

- **Compile-time schema validation**: Column names and types are checked at compile time
- **Type-safe operations**: All DataFrame operations preserve type information
- **Zero-cost abstractions**: No runtime overhead compared to vanilla Polars
- **Familiar API**: Similar to Polars with added type safety
- **Full Polars access**: Easy access to underlying Polars types when needed

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
typed_polars = "0.1.0"
polars = { version = "0.44", features = ["lazy", "dtype-full", "parquet", "csv"] }
```

## Quick Start

```rust
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
    // Create a Polars DataFrame
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3]).into_column(),
        Series::new("name".into(), vec!["Alice", "Bob", "Charlie"]).into_column(),
        Series::new("age".into(), vec![25i32, 30, 35]).into_column(),
        Series::new("salary".into(), vec![50000.0f64, 60000.0, 75000.0]).into_column(),
        Series::new("active".into(), vec![true, true, false]).into_column(),
    ])?;
    
    // Wrap it in a TypedDataFrame with schema validation
    let typed_df = TypedDataFrame::<UserSchema>::new(df)?;
    
    // Type-safe column access - compile error if column doesn't exist or has wrong type!
    let name_series = typed_df.column(UserSchema::name)?;
    let age_series = typed_df.column(UserSchema::age)?;
    
    // Sort by salary
    let sorted = typed_df.sort(UserSchema::salary, true)?;
    
    println!("{}", sorted);
    
    Ok(())
}
```

## Core Concepts

### Schema Definition

Define your DataFrame schema using the `define_schema!` macro:

```rust
typed_polars::define_schema! {
    MySchema {
        column1: i64,
        column2: String,
        column3: f64,
    }
}
```

This creates:
1. A `MySchema` type that implements the `Schema` trait
2. Const column accessors: `MySchema::column1`, `MySchema::column2`, etc.

### TypedDataFrame

`TypedDataFrame<S>` wraps a Polars `DataFrame` and carries schema information at compile time:

```rust
let typed_df = TypedDataFrame::<MySchema>::new(df)?; // Validates schema at runtime
```

### TypedSeries

`TypedSeries<T>` wraps a Polars `Series` with type information:

```rust
let series = TypedSeries::<i32>::from_vec("values", vec![1, 2, 3]);
```

### Type-Safe Column Access

Access columns with compile-time type checking:

```rust
let typed_series = typed_df.column(MySchema::column1)?; // Returns TypedSeries<i64>
```

## Examples

### CSV I/O

```rust
use typed_polars::prelude::*;

typed_polars::define_schema! {
    SalesSchema {
        product_id: i64,
        product_name: String,
        quantity: i32,
        price: f64,
    }
}

// Read CSV with schema validation
let df = CsvReader::<SalesSchema>::new("sales_data.csv")
    .has_header(true)
    .finish()?;

// Write to CSV
df.write_csv("output.csv")?;

// Write to Parquet
df.write_parquet("output.parquet")?;
```

### Lazy Evaluation

```rust
use typed_polars::prelude::*;
use typed_polars::expr::col;

let lazy_df = typed_df.lazy();

let salary_expr = col(MySchema::salary);

let result = lazy_df
    .group_by([MySchema::department.name()])
    .agg([
        salary_expr.mean().alias("avg_salary").into_inner(),
    ])
    .collect()?;
```

### DataFrame Operations

```rust
// All operations preserve the schema type
let head = typed_df.head(Some(10));
let tail = typed_df.tail(Some(10));
let sliced = typed_df.slice(0, 100);

// Sort by a column
let sorted = typed_df.sort(MySchema::salary, false)?;
```

## API Overview

### TypedDataFrame Methods

- `new(df: DataFrame)` - Create from Polars DataFrame with validation
- `column<T>(col: Column<T>)` - Get typed column
- `head(n)`, `tail(n)`, `slice(offset, length)` - Selection operations
- `sort<T>(col: Column<T>, descending)` - Sort by column
- `filter(mask)` - Filter rows
- `inner()` - Access underlying Polars DataFrame
- `lazy()` - Convert to LazyFrame

### I/O Operations

- `CsvReader<S>::new(path)` - Read CSV with schema
- `ParquetReader<S>::new(path)` - Read Parquet with schema
- `write_csv(path)` - Write to CSV
- `write_parquet(path)` - Write to Parquet

## Supported Types

- Integers: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
- Floats: `f32`, `f64`
- Boolean: `bool`
- String: `String`, `str`

## Running Examples

```bash
# Basic usage
cargo run --example basic_usage

# CSV I/O
cargo run --example csv_io

# Typed expressions
cargo run --example typed_expressions
```

## Running Tests

```bash
cargo test
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
