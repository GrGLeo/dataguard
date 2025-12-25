use dataguard_core::{CsvTable, NumericColumnBuilder, StringColumnBuilder, Table, Validator};
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_validator_add_single_table() {
    let mut validator = Validator::new();
    let table = CsvTable::new("path/to/file.csv".to_string(), "stdout".to_string()).unwrap();

    validator.add_table("users".to_string(), table);

    // No direct way to check, but validation should not panic
    // This is a basic smoke test
}

#[test]
fn test_validator_add_multiple_tables() {
    let mut validator = Validator::new();

    let table1 = CsvTable::new("path/to/users.csv".to_string(), "stdout".to_string()).unwrap();
    let table2 = CsvTable::new("path/to/orders.csv".to_string(), "json".to_string()).unwrap();
    let table3 = CsvTable::new("path/to/products.csv".to_string(), "csv".to_string()).unwrap();

    validator.add_table("users".to_string(), table1);
    validator.add_table("orders".to_string(), table2);
    validator.add_table("products".to_string(), table3);

    // HashMap should contain all three tables
}

#[test]
fn test_validator_validate_nonexistent_table() {
    let mut validator = Validator::new();

    // Try to validate a table that doesn't exist
    let result = validator.validate_table("nonexistent".to_string());

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Table 'nonexistent' not found in Validator"
    );
}

#[test]
fn test_validator_validate_existing_table() {
    // Create a temporary CSV file with valid data
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "name,age").unwrap();
    writeln!(file, "alice,25").unwrap();
    writeln!(file, "bob,30").unwrap();

    let mut name_col = StringColumnBuilder::new("name".to_string());
    name_col.with_min_length(3, 0.0);

    let mut age_col = NumericColumnBuilder::<i64>::new("age".to_string());
    age_col.is_positive(0.0);

    let file_path_str = file_path.into_os_string().into_string().unwrap();
    let mut table = CsvTable::new(file_path_str, "stdout".to_string()).unwrap();
    table
        .prepare(vec![Box::new(name_col), Box::new(age_col)], vec![])
        .unwrap();

    let mut validator = Validator::new();
    validator.add_table("users".to_string(), table);

    // Validate the existing table
    let result = validator.validate_table("users".to_string());

    assert!(result.is_ok());
}

#[test]
fn test_validator_replace_table() {
    let mut validator = Validator::new();

    let table1 = CsvTable::new("path/to/file1.csv".to_string(), "stdout".to_string()).unwrap();
    let table2 = CsvTable::new("path/to/file2.csv".to_string(), "json".to_string()).unwrap();

    // Add first table
    validator.add_table("data".to_string(), table1);

    // Replace with second table (same name)
    validator.add_table("data".to_string(), table2);

    // The second table should have replaced the first
    // This is implicitly tested by the HashMap behavior
}
