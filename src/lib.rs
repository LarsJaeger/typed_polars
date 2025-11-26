//! # Typed Polars
//!
//! A statically-typed wrapper around Polars that provides compile-time type safety
//! for DataFrame schemas and operations.
//!
//! ## Features
//!
//! - **Compile-time schema validation**: Column names and types are checked at compile time
//! - **Type-safe operations**: All DataFrame operations preserve type information
//! - **Zero-cost abstraction**: No runtime overhead compared to vanilla Polars
//! - **Familiar API**: Similar to Polars with added type safety
//!
//! ## Example
//!
//! ```ignore
//! use typed_polars::prelude::*;
//!
//! // Define a schema using a struct
//! #[derive(Schema)]
//! struct UserData {
//!     id: i64,
//!     name: String,
//!     age: i32,
//!     active: bool,
//! }
//!
//! // Create a typed DataFrame
//! let df = TypedDataFrame::<UserData>::new(df_polars)?;
//!
//! // Type-safe column access - compile error if column doesn't exist
//! let name_series = df.column::<schema::name>()?;
//! ```

pub mod prelude;
pub mod schema;
pub mod series;
pub mod dataframe;
pub mod expr;
pub mod io;

pub use dataframe::TypedDataFrame;
pub use series::TypedSeries;
pub use schema::Schema;
