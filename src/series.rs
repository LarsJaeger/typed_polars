//! Typed wrapper around Polars Series with compile-time type information.

use polars::prelude::*;
use crate::schema::ColumnType;
use std::marker::PhantomData;

/// A typed wrapper around a Polars Series that carries type information at compile time.
///
/// This ensures that operations on the series are type-safe and that the series
/// contains data of the expected type.
pub struct TypedSeries<T: ColumnType> {
    inner: Series,
    _phantom: PhantomData<T>,
}

impl<T: ColumnType> TypedSeries<T> {
    /// Create a new TypedSeries from a Polars Series.
    ///
    /// # Errors
    ///
    /// Returns an error if the series data type doesn't match the expected type.
    pub fn new(series: Series) -> PolarsResult<Self> {
        let expected_dtype = T::data_type();
        if series.dtype() != &expected_dtype {
            return Err(PolarsError::SchemaMismatch(
                format!(
                    "Series has type {:?}, expected {:?}",
                    series.dtype(),
                    expected_dtype
                ).into()
            ));
        }
        
        Ok(Self {
            inner: series,
            _phantom: PhantomData,
        })
    }
    
    /// Create a new TypedSeries without validating the type.
    ///
    /// # Safety
    ///
    /// This is unsafe because it doesn't validate that the series has the correct type.
    /// Use with caution when you're certain the type is correct.
    pub unsafe fn new_unchecked(series: Series) -> Self {
        Self {
            inner: series,
            _phantom: PhantomData,
        }
    }
    
    /// Get a reference to the underlying Polars Series.
    pub fn inner(&self) -> &Series {
        &self.inner
    }
    
    /// Consume self and return the underlying Polars Series.
    pub fn into_inner(self) -> Series {
        self.inner
    }
    
    /// Get the name of the series.
    pub fn name(&self) -> &str {
        self.inner.name()
    }
    
    /// Get the length of the series.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    
    /// Check if the series is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    
    /// Rename the series.
    pub fn rename(&mut self, name: &str) {
        self.inner.rename(name.into());
    }
}

impl<T: ColumnType> Clone for TypedSeries<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: ColumnType> std::fmt::Debug for TypedSeries<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

// Conversions from native Rust types to TypedSeries
impl TypedSeries<i32> {
    pub fn from_vec(name: &str, data: Vec<i32>) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
}

impl TypedSeries<i64> {
    pub fn from_vec(name: &str, data: Vec<i64>) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
}

impl TypedSeries<f64> {
    pub fn from_vec(name: &str, data: Vec<f64>) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
}

impl TypedSeries<f32> {
    pub fn from_vec(name: &str, data: Vec<f32>) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
}

impl TypedSeries<bool> {
    pub fn from_vec(name: &str, data: Vec<bool>) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
}

impl TypedSeries<String> {
    pub fn from_vec(name: &str, data: Vec<String>) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
    
    pub fn from_slice(name: &str, data: &[&str]) -> Self {
        Self {
            inner: Series::new(name.into(), data),
            _phantom: PhantomData,
        }
    }
}
