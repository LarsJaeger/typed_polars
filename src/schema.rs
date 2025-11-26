//! Schema type system for compile-time type safety.
//!
//! This module provides the core trait and types for defining and working with
//! typed schemas in Polars DataFrames.

use polars::prelude::*;
use std::marker::PhantomData;

/// Trait for types that represent a DataFrame schema.
///
/// This trait is typically implemented via a derive macro on a struct,
/// where each field represents a column in the DataFrame.
pub trait Schema: Sized {
    /// Returns the schema as a Polars Schema
    fn schema() -> polars::prelude::Schema;
    
    /// Returns all column names in order
    fn column_names() -> Vec<&'static str>;
    
    /// Validates that a DataFrame matches this schema
    fn validate(df: &DataFrame) -> PolarsResult<()>;
}

/// Marker trait for column types that can be used in a schema
pub trait ColumnType {
    /// The corresponding Polars DataType
    fn data_type() -> DataType;
}

// Implement ColumnType for common Rust types
impl ColumnType for i8 {
    fn data_type() -> DataType { DataType::Int8 }
}

impl ColumnType for i16 {
    fn data_type() -> DataType { DataType::Int16 }
}

impl ColumnType for i32 {
    fn data_type() -> DataType { DataType::Int32 }
}

impl ColumnType for i64 {
    fn data_type() -> DataType { DataType::Int64 }
}

impl ColumnType for u8 {
    fn data_type() -> DataType { DataType::UInt8 }
}

impl ColumnType for u16 {
    fn data_type() -> DataType { DataType::UInt16 }
}

impl ColumnType for u32 {
    fn data_type() -> DataType { DataType::UInt32 }
}

impl ColumnType for u64 {
    fn data_type() -> DataType { DataType::UInt64 }
}

impl ColumnType for f32 {
    fn data_type() -> DataType { DataType::Float32 }
}

impl ColumnType for f64 {
    fn data_type() -> DataType { DataType::Float64 }
}

impl ColumnType for bool {
    fn data_type() -> DataType { DataType::Boolean }
}

impl ColumnType for String {
    fn data_type() -> DataType { DataType::String }
}

impl ColumnType for str {
    fn data_type() -> DataType { DataType::String }
}

/// Marker type for a specific column in a schema
///
/// This allows compile-time verification that a column exists and has the correct type.
pub struct Column<T: ColumnType> {
    pub name: &'static str,
    _phantom: PhantomData<T>,
}

impl<T: ColumnType> Column<T> {
    /// Create a new column marker
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _phantom: PhantomData,
        }
    }
    
    /// Get the column name
    pub fn name(&self) -> &'static str {
        self.name
    }
    
    /// Get the data type
    pub fn data_type(&self) -> DataType {
        T::data_type()
    }
}

impl<T: ColumnType> Clone for Column<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            _phantom: PhantomData,
        }
    }
}

impl<T: ColumnType> Copy for Column<T> {}

/// Macro to define a schema with compile-time type information
///
/// # Example
///
/// ```ignore
/// define_schema! {
///     UserSchema {
///         id: i64,
///         name: String,
///         age: i32,
///         active: bool,
///     }
/// }
/// ```
#[macro_export]
macro_rules! define_schema {
    (
        $schema_name:ident {
            $($field_name:ident: $field_type:ty),* $(,)?
        }
    ) => {
        pub struct $schema_name;
        
        impl $crate::schema::Schema for $schema_name {
            fn schema() -> polars::prelude::Schema {
                use polars::prelude::*;
                use $crate::schema::ColumnType;
                
                Schema::from_iter(vec![
                    $(
                        Field::new(stringify!($field_name).into(), <$field_type>::data_type()),
                    )*
                ])
            }
            
            fn column_names() -> Vec<&'static str> {
                vec![
                    $(stringify!($field_name),)*
                ]
            }
            
            fn validate(df: &DataFrame) -> PolarsResult<()> {
                let expected_schema = Self::schema();
                let actual_schema = df.schema();
                
                for (name, expected_dtype) in expected_schema.iter() {
                    match actual_schema.get(name) {
                        Some(actual_dtype) if actual_dtype == expected_dtype => {},
                        Some(actual_dtype) => {
                            return Err(PolarsError::SchemaMismatch(
                                format!(
                                    "Column '{}' has type {:?}, expected {:?}",
                                    name, actual_dtype, expected_dtype
                                ).into()
                            ));
                        }
                        None => {
                            return Err(PolarsError::ColumnNotFound(
                                format!("Column '{}' not found in DataFrame", name).into()
                            ));
                        }
                    }
                }
                
                Ok(())
            }
        }
        
        // Create column accessors directly on the schema struct
        #[allow(non_upper_case_globals)]
        impl $schema_name {
            $(
                pub const $field_name: $crate::schema::Column<$field_type> = $crate::schema::Column::new(stringify!($field_name));
            )*
        }
    };
}
