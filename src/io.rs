//! I/O operations for reading and writing typed DataFrames.

use polars::prelude::*;
use crate::schema::Schema;
use crate::dataframe::TypedDataFrame;
use std::path::Path;

/// Reader for CSV files with schema validation.
pub struct CsvReader<Sch: Schema> {
    path: String,
    has_header: bool,
    _phantom: std::marker::PhantomData<Sch>,
}

impl<Sch: Schema> CsvReader<Sch> {
    /// Create a new CSV reader for the given path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_string_lossy().to_string(),
            has_header: true,
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Set whether the CSV has a header row (default: true).
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }
    
    /// Read the CSV file and validate it against the schema.
    pub fn finish(self) -> PolarsResult<TypedDataFrame<Sch>> {
        let df = CsvReadOptions::default()
            .with_has_header(self.has_header)
            .with_schema(Some(std::sync::Arc::new(Sch::schema())))
            .try_into_reader_with_file_path(Some(self.path.into()))?
            .finish()?;
        
        TypedDataFrame::new(df)
    }
}

/// Writer for CSV files.
pub struct CsvWriter<'a, Sch: Schema> {
    df: &'a TypedDataFrame<Sch>,
    has_header: bool,
}

impl<'a, Sch: Schema> CsvWriter<'a, Sch> {
    /// Create a new CSV writer for the given DataFrame.
    pub fn new(df: &'a TypedDataFrame<Sch>) -> Self {
        Self {
            df,
            has_header: true,
        }
    }
    
    /// Set whether to write a header row (default: true).
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }
    
    /// Write the DataFrame to a CSV file.
    pub fn finish(self, path: impl AsRef<Path>) -> PolarsResult<()> {
        let mut file = std::fs::File::create(path)?;
        let mut df_clone = self.df.inner().clone();
        polars::prelude::CsvWriter::new(&mut file)
            .include_header(self.has_header)
            .finish(&mut df_clone)?;
        Ok(())
    }
}

/// Reader for Parquet files with schema validation.
pub struct ParquetReader<Sch: Schema> {
    path: String,
    _phantom: std::marker::PhantomData<Sch>,
}

impl<Sch: Schema> ParquetReader<Sch> {
    /// Create a new Parquet reader for the given path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_string_lossy().to_string(),
            _phantom: std::marker::PhantomData,
        }
    }
    
    /// Read the Parquet file and validate it against the schema.
    pub fn finish(self) -> PolarsResult<TypedDataFrame<Sch>> {
        let file = std::fs::File::open(&self.path)?;
        let df = polars::prelude::ParquetReader::new(file).finish()?;
        
        TypedDataFrame::new(df)
    }
}

/// Writer for Parquet files.
pub struct ParquetWriter<'a, Sch: Schema> {
    df: &'a TypedDataFrame<Sch>,
}

impl<'a, Sch: Schema> ParquetWriter<'a, Sch> {
    /// Create a new Parquet writer for the given DataFrame.
    pub fn new(df: &'a TypedDataFrame<Sch>) -> Self {
        Self { df }
    }
    
    /// Write the DataFrame to a Parquet file.
    pub fn finish(self, path: impl AsRef<Path>) -> PolarsResult<()> {
        let mut file = std::fs::File::create(path)?;
        let mut df_clone = self.df.inner().clone();
        polars::prelude::ParquetWriter::new(&mut file)
            .finish(&mut df_clone)?;
        Ok(())
    }
}

/// Extension trait for TypedDataFrame to add I/O convenience methods.
pub trait TypedDataFrameIo<Sch: Schema> {
    /// Write this DataFrame to a CSV file.
    fn write_csv(&self, path: impl AsRef<Path>) -> PolarsResult<()>;
    
    /// Write this DataFrame to a Parquet file.
    fn write_parquet(&self, path: impl AsRef<Path>) -> PolarsResult<()>;
}

impl<Sch: Schema> TypedDataFrameIo<Sch> for TypedDataFrame<Sch> {
    fn write_csv(&self, path: impl AsRef<Path>) -> PolarsResult<()> {
        CsvWriter::new(self).finish(path)
    }
    
    fn write_parquet(&self, path: impl AsRef<Path>) -> PolarsResult<()> {
        ParquetWriter::new(self).finish(path)
    }
}
