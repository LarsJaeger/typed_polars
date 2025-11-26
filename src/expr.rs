//! Type-safe expression builder for lazy DataFrame operations.

use polars::prelude::*;
use crate::schema::{Column, ColumnType};
use std::marker::PhantomData;

/// A typed wrapper around Polars expressions that preserves type information.
///
/// This allows for type-safe construction of lazy queries while maintaining
/// compile-time guarantees about column types.
pub struct TypedExpr<T: ColumnType> {
    inner: Expr,
    _phantom: PhantomData<T>,
}

impl<T: ColumnType> TypedExpr<T> {
    /// Create a new TypedExpr from a Polars expression.
    pub fn new(expr: Expr) -> Self {
        Self {
            inner: expr,
            _phantom: PhantomData,
        }
    }
    
    /// Get the underlying Polars expression.
    pub fn inner(&self) -> &Expr {
        &self.inner
    }
    
    /// Consume self and return the underlying expression.
    pub fn into_inner(self) -> Expr {
        self.inner
    }
    
    /// Alias the expression with a new name.
    pub fn alias(self, name: &str) -> Self {
        Self::new(self.inner.alias(name))
    }
}

impl<T: ColumnType> Clone for TypedExpr<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Create a typed column expression.
pub fn col<T: ColumnType>(column: Column<T>) -> TypedExpr<T> {
    TypedExpr::new(polars::prelude::col(column.name()))
}

// Numeric operations
impl TypedExpr<i32> {
    pub fn add(self, other: TypedExpr<i32>) -> Self {
        Self::new(self.inner + other.inner)
    }
    
    pub fn sub(self, other: TypedExpr<i32>) -> Self {
        Self::new(self.inner - other.inner)
    }
    
    pub fn mul(self, other: TypedExpr<i32>) -> Self {
        Self::new(self.inner * other.inner)
    }
    
    pub fn div(self, other: TypedExpr<i32>) -> Self {
        Self::new(self.inner / other.inner)
    }
    
    pub fn sum(self) -> Self {
        Self::new(self.inner.sum())
    }
    
    pub fn mean(self) -> Self {
        Self::new(self.inner.mean())
    }
    
    pub fn min(self) -> Self {
        Self::new(self.inner.min())
    }
    
    pub fn max(self) -> Self {
        Self::new(self.inner.max())
    }
}

impl TypedExpr<i64> {
    pub fn add(self, other: TypedExpr<i64>) -> Self {
        Self::new(self.inner + other.inner)
    }
    
    pub fn sub(self, other: TypedExpr<i64>) -> Self {
        Self::new(self.inner - other.inner)
    }
    
    pub fn mul(self, other: TypedExpr<i64>) -> Self {
        Self::new(self.inner * other.inner)
    }
    
    pub fn div(self, other: TypedExpr<i64>) -> Self {
        Self::new(self.inner / other.inner)
    }
    
    pub fn sum(self) -> Self {
        Self::new(self.inner.sum())
    }
    
    pub fn mean(self) -> Self {
        Self::new(self.inner.mean())
    }
    
    pub fn min(self) -> Self {
        Self::new(self.inner.min())
    }
    
    pub fn max(self) -> Self {
        Self::new(self.inner.max())
    }
}

impl TypedExpr<f64> {
    pub fn add(self, other: TypedExpr<f64>) -> Self {
        Self::new(self.inner + other.inner)
    }
    
    pub fn sub(self, other: TypedExpr<f64>) -> Self {
        Self::new(self.inner - other.inner)
    }
    
    pub fn mul(self, other: TypedExpr<f64>) -> Self {
        Self::new(self.inner * other.inner)
    }
    
    pub fn div(self, other: TypedExpr<f64>) -> Self {
        Self::new(self.inner / other.inner)
    }
    
    pub fn sum(self) -> Self {
        Self::new(self.inner.sum())
    }
    
    pub fn mean(self) -> Self {
        Self::new(self.inner.mean())
    }
    
    pub fn min(self) -> Self {
        Self::new(self.inner.min())
    }
    
    pub fn max(self) -> Self {
        Self::new(self.inner.max())
    }
}

impl TypedExpr<f32> {
    pub fn add(self, other: TypedExpr<f32>) -> Self {
        Self::new(self.inner + other.inner)
    }
    
    pub fn sub(self, other: TypedExpr<f32>) -> Self {
        Self::new(self.inner - other.inner)
    }
    
    pub fn mul(self, other: TypedExpr<f32>) -> Self {
        Self::new(self.inner * other.inner)
    }
    
    pub fn div(self, other: TypedExpr<f32>) -> Self {
        Self::new(self.inner / other.inner)
    }
    
    pub fn sum(self) -> Self {
        Self::new(self.inner.sum())
    }
    
    pub fn mean(self) -> Self {
        Self::new(self.inner.mean())
    }
    
    pub fn min(self) -> Self {
        Self::new(self.inner.min())
    }
    
    pub fn max(self) -> Self {
        Self::new(self.inner.max())
    }
}

// String operations
impl TypedExpr<String> {
    // Note: String operations require the "strings" feature and newer API
    // For now, we'll provide basic conversions and users can use the inner expression
    // for more complex string operations
}

// Boolean operations
impl TypedExpr<bool> {
    pub fn and(self, other: TypedExpr<bool>) -> Self {
        Self::new(self.inner.and(other.inner))
    }
    
    pub fn or(self, other: TypedExpr<bool>) -> Self {
        Self::new(self.inner.or(other.inner))
    }
    
    pub fn not(self) -> Self {
        Self::new(self.inner.not())
    }
}
