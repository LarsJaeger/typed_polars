//! Prelude module for convenient imports.
//!
//! This module re-exports the most commonly used types and traits
//! for working with typed Polars DataFrames.

pub use crate::schema::{Schema, Column, ColumnType};
pub use crate::series::TypedSeries;
pub use crate::dataframe::TypedDataFrame;
pub use crate::expr::{TypedExpr, col};
pub use crate::io::{CsvReader, CsvWriter, ParquetReader, ParquetWriter, TypedDataFrameIo};
pub use crate::define_schema;

// Re-export commonly used Polars types
pub use polars::prelude::{
    DataFrame, Series, PolarsResult, PolarsError,
    DataType, AnyValue, ChunkedArray, BooleanType,
    IdxCa, LazyFrame, NamedFrom, IntoColumn,
};
