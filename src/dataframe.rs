//! Typed wrapper around Polars DataFrame with compile-time schema validation.

use polars::prelude::*;
use crate::schema::{Schema, Column, ColumnType};
use crate::series::TypedSeries;
use std::marker::PhantomData;

/// A typed wrapper around a Polars DataFrame that carries schema information at compile time.
///
/// This ensures that all operations on the DataFrame are validated against the schema
/// at compile time, preventing runtime errors from missing or mistyped columns.
pub struct TypedDataFrame<S: Schema> {
    inner: DataFrame,
    _phantom: PhantomData<S>,
}

impl<S: Schema> TypedDataFrame<S> {
    /// Create a new TypedDataFrame from a Polars DataFrame.
    ///
    /// # Errors
    ///
    /// Returns an error if the DataFrame doesn't match the expected schema.
    pub fn new(df: DataFrame) -> PolarsResult<Self> {
        S::validate(&df)?;
        
        Ok(Self {
            inner: df,
            _phantom: PhantomData,
        })
    }
    
    /// Create a new TypedDataFrame without validating the schema.
    ///
    /// # Safety
    ///
    /// This is unsafe because it doesn't validate that the DataFrame matches the schema.
    /// Use with caution when you're certain the schema is correct.
    pub unsafe fn new_unchecked(df: DataFrame) -> Self {
        Self {
            inner: df,
            _phantom: PhantomData,
        }
    }
    
    /// Get a reference to the underlying Polars DataFrame.
    pub fn inner(&self) -> &DataFrame {
        &self.inner
    }
    
    /// Consume self and return the underlying Polars DataFrame.
    pub fn into_inner(self) -> DataFrame {
        self.inner
    }
    
    /// Get the schema of this DataFrame.
    pub fn schema() -> polars::prelude::Schema {
        S::schema()
    }
    
    /// Get the number of rows in the DataFrame.
    pub fn height(&self) -> usize {
        self.inner.height()
    }
    
    /// Get the number of columns in the DataFrame.
    pub fn width(&self) -> usize {
        self.inner.width()
    }
    
    /// Get the shape of the DataFrame as (height, width).
    pub fn shape(&self) -> (usize, usize) {
        self.inner.shape()
    }
    
    /// Check if the DataFrame is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    
    /// Get a typed column from the DataFrame.
    ///
    /// This method provides compile-time verification that the column exists
    /// and has the correct type.
    pub fn column<T: ColumnType>(&self, col: crate::schema::Column<T>) -> PolarsResult<TypedSeries<T>> {
        let column = self.inner.column(col.name())?;
        let series = column.as_materialized_series().clone();
        TypedSeries::new(series)
    }
    
    /// Select specific columns from the DataFrame.
    ///
    /// Note: This returns an untyped DataFrame since the selection might not
    /// match the original schema.
    pub fn select(&self, columns: Vec<&str>) -> PolarsResult<DataFrame> {
        self.inner.select(columns)
    }
    
    /// Filter the DataFrame using a boolean mask.
    pub fn filter(&self, mask: &ChunkedArray<BooleanType>) -> PolarsResult<Self> {
        let filtered = self.inner.filter(mask)?;
        unsafe { Ok(Self::new_unchecked(filtered)) }
    }
    
    /// Take rows by index.
    pub fn take(&self, indices: &IdxCa) -> PolarsResult<Self> {
        let taken = self.inner.take(indices)?;
        unsafe { Ok(Self::new_unchecked(taken)) }
    }
    
    /// Sort the DataFrame by a column.
    pub fn sort<T: ColumnType>(
        &self,
        col: Column<T>,
        descending: bool,
    ) -> PolarsResult<Self> {
        let sorted = self.inner.sort(
            vec![col.name()],
            SortMultipleOptions::default().with_order_descending(descending)
        )?;
        unsafe { Ok(Self::new_unchecked(sorted)) }
    }
    
    /// Get the head of the DataFrame (first n rows).
    pub fn head(&self, n: Option<usize>) -> Self {
        let head = self.inner.head(n);
        unsafe { Self::new_unchecked(head) }
    }
    
    /// Get the tail of the DataFrame (last n rows).
    pub fn tail(&self, n: Option<usize>) -> Self {
        let tail = self.inner.tail(n);
        unsafe { Self::new_unchecked(tail) }
    }
    
    /// Slice the DataFrame.
    pub fn slice(&self, offset: i64, length: usize) -> Self {
        let sliced = self.inner.slice(offset, length);
        unsafe { Self::new_unchecked(sliced) }
    }
    
    /// Convert the DataFrame to a lazy DataFrame for query optimization.
    pub fn lazy(self) -> LazyFrame {
        self.inner.lazy()
    }
}

impl<S: Schema> Clone for TypedDataFrame<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<S: Schema> std::fmt::Debug for TypedDataFrame<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<S: Schema> std::fmt::Display for TypedDataFrame<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
