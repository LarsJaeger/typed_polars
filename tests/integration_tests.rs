use typed_polars::prelude::*;

// Define test schema
typed_polars::define_schema! {
    TestSchema {
        id: i64,
        name: String,
        value: i32,
    }
}

#[test]
fn test_create_typed_dataframe() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3]).into_column(),
        Series::new("name".into(), vec!["a", "b", "c"]).into_column(),
        Series::new("value".into(), vec![10i32, 20, 30]).into_column(),
    ]).unwrap();
    
    let typed_df = TypedDataFrame::<TestSchema>::new(df);
    assert!(typed_df.is_ok());
}

#[test]
fn test_schema_validation_fails_on_wrong_type() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3]).into_column(),
        Series::new("name".into(), vec!["a", "b", "c"]).into_column(),
        Series::new("value".into(), vec![10.5f64, 20.5, 30.5]).into_column(), // Wrong type!
    ]).unwrap();
    
    let typed_df = TypedDataFrame::<TestSchema>::new(df);
    assert!(typed_df.is_err());
}

#[test]
fn test_schema_validation_fails_on_missing_column() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3]).into_column(),
        Series::new("name".into(), vec!["a", "b", "c"]).into_column(),
        // Missing "value" column
    ]).unwrap();
    
    let typed_df = TypedDataFrame::<TestSchema>::new(df);
    assert!(typed_df.is_err());
}

#[test]
fn test_typed_column_access() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3]).into_column(),
        Series::new("name".into(), vec!["a", "b", "c"]).into_column(),
        Series::new("value".into(), vec![10i32, 20, 30]).into_column(),
    ]).unwrap();
    
    let typed_df = TypedDataFrame::<TestSchema>::new(df).unwrap();
    
    let id_col = typed_df.column(TestSchema::id);
    assert!(id_col.is_ok());
    assert_eq!(id_col.unwrap().len(), 3);
    
    let name_col = typed_df.column(TestSchema::name);
    assert!(name_col.is_ok());
    assert_eq!(name_col.unwrap().len(), 3);
}

#[test]
fn test_typed_series_creation() {
    let series = TypedSeries::<i32>::from_vec("test", vec![1, 2, 3]);
    assert_eq!(series.len(), 3);
    assert_eq!(series.name(), "test");
}

#[test]
fn test_dataframe_operations() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![1i64, 2, 3, 4, 5]).into_column(),
        Series::new("name".into(), vec!["a", "b", "c", "d", "e"]).into_column(),
        Series::new("value".into(), vec![10i32, 20, 30, 40, 50]).into_column(),
    ]).unwrap();
    
    let typed_df = TypedDataFrame::<TestSchema>::new(df).unwrap();
    
    // Test head
    let head = typed_df.head(Some(2));
    assert_eq!(head.height(), 2);
    
    // Test tail
    let tail = typed_df.tail(Some(2));
    assert_eq!(tail.height(), 2);
    
    // Test slice
    let sliced = typed_df.slice(1, 2);
    assert_eq!(sliced.height(), 2);
}

#[test]
fn test_dataframe_sort() {
    let df = DataFrame::new(vec![
        Series::new("id".into(), vec![3i64, 1, 2]).into_column(),
        Series::new("name".into(), vec!["c", "a", "b"]).into_column(),
        Series::new("value".into(), vec![30i32, 10, 20]).into_column(),
    ]).unwrap();
    
    let typed_df = TypedDataFrame::<TestSchema>::new(df).unwrap();
    let sorted = typed_df.sort(TestSchema::value, false).unwrap();
    
    assert_eq!(sorted.height(), 3);
}
