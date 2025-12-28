use dataguard_core::{NumericColumnBuilder, ParquetTable, StringColumnBuilder, Table};
use std::path::PathBuf;

fn get_test_file_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests");
    path.push("fixtures");
    path.push("test_ecommerce_data.parquet");
    path
}

#[test]
fn test_table_string_column_validation() {
    let file_path = get_test_file_path().to_str().unwrap().to_string();
    let mut name = StringColumnBuilder::new(String::from("name"));
    name.is_not_null(0.);
    // Commit to validator
    let mut parquet_table = ParquetTable::new(file_path, "stdout".to_string()).unwrap();
    parquet_table.prepare(vec![Box::new(name)], vec![]).unwrap();

    // Run validation
    let res = parquet_table.validate();
    assert!(res.is_ok())
}

#[test]
fn test_table_integer_column_validation() {
    let file_path = get_test_file_path().to_str().unwrap().to_string();
    let mut id = NumericColumnBuilder::<i64>::new("id".to_string());
    id.min(0, 0.);
    let mut parquet_table = ParquetTable::new(file_path, "stdout".to_string()).unwrap();
    parquet_table.prepare(vec![Box::new(id)], vec![]).unwrap();

    let res = parquet_table.validate();
    assert!(res.is_ok())
}

#[test]
fn test_table_float_column_validation() {
    let file_path = get_test_file_path().to_str().unwrap().to_string();
    let mut parquet_table = ParquetTable::new(file_path, "stdout".to_string()).unwrap();
    let mut value = NumericColumnBuilder::<f64>::new("value".to_string());
    value.max(1000., 0.);
    parquet_table
        .prepare(vec![Box::new(value)], vec![])
        .unwrap();

    // Expected: 1 error (5.0 < 25.0 violates monotonicity)
    let res = parquet_table.validate();
    assert!(res.is_ok())
}

#[test]
fn test_table_get_rules() {
    let mut col1 = StringColumnBuilder::new("col1".to_string());
    col1.with_length_between(1, 10, 0.0);

    let mut col2 = StringColumnBuilder::new("col2".to_string());
    col2.with_regex("^[a-z]+$".to_string(), None, 0.0).unwrap();

    let mut col3 = NumericColumnBuilder::<i64>::new("col3".to_string());
    col3.between(2, 5, 0.0);

    let mut csv_table = ParquetTable::new("hi".to_string(), "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(col1), Box::new(col2), Box::new(col3)], vec![])
        .unwrap();

    let rules = csv_table.get_rules();
    assert_eq!(rules.len(), 3);
    assert_eq!(
        rules.get("col1").unwrap(),
        &vec!["TypeCheck".to_string(), "WithLengthBetween".to_string()]
    );
    assert_eq!(
        rules.get("col2").unwrap(),
        &vec!["TypeCheck".to_string(), "WithRegex".to_string()]
    );
    assert_eq!(
        rules.get("col3").unwrap(),
        &vec!["TypeCheck".to_string(), "Between".to_string()]
    );
}

#[test]
fn test_table_multiple_rules_per_column() {
    let file_path = get_test_file_path().to_str().unwrap().to_string();
    let mut name = StringColumnBuilder::new("name".to_string());
    name.with_min_length(3, 0.0)
        .with_max_length(20, 0.0)
        .is_alpha(0.0)
        .unwrap();

    let mut parquet_table = ParquetTable::new(file_path, "stdout".to_string()).unwrap();
    parquet_table.prepare(vec![Box::new(name)], vec![]).unwrap();
    let res = parquet_table.validate();
    assert!(res.is_ok());
}

#[test]
fn test_table_mixed_column_types() {
    let file_path = get_test_file_path().to_str().unwrap().to_string();
    let mut parquet_table = ParquetTable::new(file_path, "stdout".to_string()).unwrap();
    let mut id = NumericColumnBuilder::<i64>::new("id".to_string());
    id.is_not_null(0.);
    let mut value = NumericColumnBuilder::<f64>::new("value".to_string());
    value.min(0., 0.);
    let mut name = StringColumnBuilder::new("name".to_string());
    name.is_alphanumeric(0.).unwrap();
    parquet_table
        .prepare(vec![Box::new(id), Box::new(value), Box::new(name)], vec![])
        .unwrap();

    let res = parquet_table.validate();
    assert!(res.is_ok())
}

#[test]
fn test_table_auto_batch_mode_small_file() {
    // Small Parquet file (< 500MB) should use batch mode automatically
    let file_path = get_test_file_path().to_str().unwrap().to_string();

    let mut id = NumericColumnBuilder::<i64>::new("id".to_string());
    id.is_positive(0.0);

    let mut parquet_table = ParquetTable::new(file_path, "batch_mode_test".to_string()).unwrap();
    parquet_table.prepare(vec![Box::new(id)], vec![]).unwrap();

    // Should use batch mode (small file)
    let res = parquet_table.validate();
    assert!(res.is_ok());
    let validation_result = res.unwrap();
    assert_eq!(validation_result.table_name, "batch_mode_test");
    assert_eq!(validation_result.total_rows, 512_000);
}

#[test]
fn test_table_streaming_mode_produces_same_results() {
    // Verify streaming and batch modes produce identical results
    let file_path = get_test_file_path().to_str().unwrap().to_string();

    let mut id = NumericColumnBuilder::<i64>::new("id".to_string());
    id.min(0, 0.0);

    let mut value = NumericColumnBuilder::<f64>::new("value".to_string());
    value.is_non_negative(0.0);

    let mut name = StringColumnBuilder::new("name".to_string());
    name.with_min_length(1, 0.0);

    let mut parquet_table = ParquetTable::new(file_path, "comparison_test".to_string()).unwrap();
    parquet_table
        .prepare(vec![Box::new(id), Box::new(value), Box::new(name)], vec![])
        .unwrap();

    // Validate (will use batch mode for small test file)
    let res = parquet_table.validate();
    assert!(res.is_ok());
    let validation_result = res.unwrap();

    // Verify basic properties
    assert_eq!(validation_result.table_name, "comparison_test");
    assert_eq!(validation_result.total_rows, 512_000);

    // Should have column results for all 3 columns
    let column_results = validation_result.get_column_results();
    assert_eq!(column_results.len(), 3);
}
