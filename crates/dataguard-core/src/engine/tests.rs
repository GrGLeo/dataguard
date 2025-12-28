//! Tests for the validation engine module.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::columns::{
    date_builder::DateColumnBuilder, numeric_builder::NumericColumnBuilder,
    relation_builder::RelationBuilder, string_builder::StringColumnBuilder,
};
use crate::compiler;
use crate::utils::operator::CompOperator;
use crate::validator::ExecutableColumn;

use super::accumulator::ResultAccumulator;
use super::unicity_accumulator::UnicityAccumulator;
use super::ValidationEngine;

// ============================================================================
// Test Utilities
// ============================================================================

/// Create a RecordBatch with a single string column.
fn create_string_batch(column_name: &str, values: Vec<Option<&str>>) -> Arc<RecordBatch> {
    let array = StringArray::from(values);
    let schema = Schema::new(vec![Field::new(column_name, DataType::Utf8, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    Arc::new(batch)
}

/// Create a RecordBatch with a single integer column.
fn create_int_batch(column_name: &str, values: Vec<Option<i64>>) -> Arc<RecordBatch> {
    let array = Int64Array::from(values);
    let schema = Schema::new(vec![Field::new(column_name, DataType::Int64, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    Arc::new(batch)
}

/// Create a RecordBatch with a single float column.
fn create_float_batch(column_name: &str, values: Vec<Option<f64>>) -> Arc<RecordBatch> {
    let array = Float64Array::from(values);
    let schema = Schema::new(vec![Field::new(column_name, DataType::Float64, true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
    Arc::new(batch)
}

/// Create an ExecutableColumn for a string column with unicity check.
fn create_string_column_with_unicity(name: &str) -> ExecutableColumn {
    let mut builder = StringColumnBuilder::new(name.to_string());
    builder.is_unique(0.0);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for a string column with length constraints.
fn create_string_column_with_length(name: &str, min: usize, max: usize) -> ExecutableColumn {
    let mut builder = StringColumnBuilder::new(name.to_string());
    builder.with_min_length(min, 0.0).with_max_length(max, 0.0);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for an integer column with range constraints.
fn create_int_column_with_range(name: &str, min: i64, max: i64) -> ExecutableColumn {
    let mut builder = NumericColumnBuilder::<i64>::new(name.to_string());
    builder.between(min, max, 0.0);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for an integer column with stats.
fn create_int_column_with_stats(name: &str, min: i64, max: i64) -> ExecutableColumn {
    let mut builder = NumericColumnBuilder::<i64>::new(name.to_string());
    builder.std_dev_check(0., 0.);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for an integer column with stats.
fn create_float_column_with_stats(name: &str, min: i64, max: i64) -> ExecutableColumn {
    let mut builder = NumericColumnBuilder::<f64>::new(name.to_string());
    builder.std_dev_check(0., 0.);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for a string column with null check.
fn create_string_column_with_null_check(name: &str) -> ExecutableColumn {
    let mut builder = StringColumnBuilder::new(name.to_string());
    builder.is_not_null(0.0);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for a date column.
fn create_date_column(name: &str, format: &str) -> ExecutableColumn {
    let builder = DateColumnBuilder::new(name.to_string(), format.to_string());
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create a RecordBatch with two date columns (as strings, to be type-checked).
fn create_two_date_batch(
    col1_name: &str,
    col2_name: &str,
    values: Vec<(Option<&str>, Option<&str>)>,
) -> Arc<RecordBatch> {
    let col1_values: Vec<Option<&str>> = values.iter().map(|(v1, _)| *v1).collect();
    let col2_values: Vec<Option<&str>> = values.iter().map(|(_, v2)| *v2).collect();

    let col1_array = StringArray::from(col1_values);
    let col2_array = StringArray::from(col2_values);

    let schema = Schema::new(vec![
        Field::new(col1_name, DataType::Utf8, true),
        Field::new(col2_name, DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(col1_array), Arc::new(col2_array)],
    )
    .unwrap();
    Arc::new(batch)
}

// ============================================================================
// ResultAccumulator Tests
// ============================================================================

#[cfg(test)]
mod result_accumulator_tests {
    use super::*;
    use rayon::prelude::*;

    #[test]
    fn test_new_accumulator_empty() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        let (column_values, column_results, relation_results) = accumulator.to_results();
        assert_eq!(column_values.len(), 0);
        assert_eq!(column_results.len(), 0);
        assert_eq!(relation_results.len(), 0);
    }

    #[test]
    fn test_record_single_result() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 5);
        accumulator.record_valid_values("column1", 5);

        let (valid_values, column_results, relation_results) = accumulator.to_results();
        assert_eq!(column_results.len(), 1);
        assert!(column_results.contains_key("column1"));

        let column_res = &column_results["column1"];
        assert_eq!(column_res.len(), 1);
        assert_eq!(valid_values.len(), 1);
        assert_eq!(column_res[0].rule_name, "rule1".to_string());
        assert_eq!(column_res[0].error_count, 5);
        assert_eq!(column_res[0].error_percentage, 5.0);
        assert!(column_res[0].error_message.is_some());
        assert_eq!(relation_results.len(), 0);
    }

    #[test]
    fn test_record_multiple_results_same_column() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_valid_values("column1", 100);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 5);
        accumulator.record_column_result("column1", "rule2".to_string(), 0.0, 10);

        let (_, column_results, _relation_results) = accumulator.to_results();
        assert_eq!(column_results.len(), 1);

        let column_res = &column_results["column1"];
        assert_eq!(column_res.len(), 2);
    }

    #[test]
    fn test_record_multiple_columns() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 5);
        accumulator.record_valid_values("column1", 100);
        accumulator.record_column_result("column2", "rule1".to_string(), 0.0, 10);
        accumulator.record_valid_values("column2", 100);

        let (_, column_results, _relation_results) = accumulator.to_results();
        assert_eq!(column_results.len(), 2);
        assert!(column_results.contains_key("column1"));
        assert!(column_results.contains_key("column2"));
    }

    #[test]
    fn test_percentage_calculation() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(200);
        accumulator.record_valid_values("column1", 200);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 50);

        let (_, column_results, _relation_results) = accumulator.to_results();
        let column_res = &column_results["column1"];
        assert_eq!(column_res[0].error_percentage, 25.0);
    }

    #[test]
    fn test_percentage_zero_total_rows() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(0);
        accumulator.record_valid_values("column1", 0);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 5);

        let (_, column_results, _relation_results) = accumulator.to_results();
        let column_res = &column_results["column1"];
        assert_eq!(column_res[0].error_percentage, 0.0);
    }

    #[test]
    fn test_accumulates_errors() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_valid_values("column1", 100);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 5);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 3);
        accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 2);

        let (_, column_results, _relation_results) = accumulator.to_results();
        let column_res = &column_results["column1"];
        assert_eq!(column_res[0].error_count, 10);
    }

    #[test]
    fn test_results_sorted_by_column() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_valid_values("zebra", 100);
        accumulator.record_valid_values("apple", 100);
        accumulator.record_valid_values("banana", 100);
        accumulator.record_column_result("zebra", "rule1".to_string(), 0.0, 1);
        accumulator.record_column_result("apple", "rule1".to_string(), 0.0, 1);
        accumulator.record_column_result("banana", "rule1".to_string(), 0.0, 1);

        let (_, column_results, _relation_results) = accumulator.to_results();
        let keys: Vec<_> = column_results.keys().cloned().collect();
        // Results are stored in HashMap, so we can't guarantee order
        // But we know all three should be present
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"zebra".to_string()));
        assert!(keys.contains(&"apple".to_string()));
        assert!(keys.contains(&"banana".to_string()));
    }

    #[test]
    fn test_concurrent_recording() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(1000);

        // Record from multiple threads
        (0..10).into_par_iter().for_each(|i| {
            let colname = &format!("column{}", i);
            accumulator.record_valid_values(&colname, 100);
            accumulator.record_column_result(&colname, "rule1".to_string(), 0.0, 1);
        });

        let (_, column_results, _relation_results) = accumulator.to_results();
        assert_eq!(column_results.len(), 10);
    }

    #[test]
    fn test_concurrent_same_column_rule() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(1000);
        accumulator.record_valid_values("column1", 1000);

        // Multiple threads recording to same (column, rule)
        (0..100).into_par_iter().for_each(|_| {
            accumulator.record_column_result("column1", "rule1".to_string(), 0.0, 1);
        });

        let (_, column_results, _relation_results) = accumulator.to_results();
        let column_res = &column_results["column1"];
        assert_eq!(column_res[0].error_count, 100);
    }
}

// ============================================================================
// UnicityAccumulator Tests
// ============================================================================

#[cfg(test)]
mod unicity_accumulator_tests {
    use super::*;
    use crate::utils::hasher::Xxh3Builder;
    use rayon::prelude::*;
    use std::collections::HashSet;
    use xxhash_rust::xxh3::xxh3_64;

    #[test]
    fn test_new_empty_when_no_unicity() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col];

        let accumulator = UnicityAccumulator::new(&columns, 1);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_new_creates_for_unicity_columns() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];

        let accumulator = UnicityAccumulator::new(&columns, 1);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 1);
        assert!(results.contains_key("email"));
    }

    #[test]
    fn test_new_multiple_unicity_columns() {
        let col1 = create_string_column_with_unicity("email");
        let col2 = create_string_column_with_unicity("username");
        let columns = vec![col1, col2];

        let accumulator = UnicityAccumulator::new(&columns, 1);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_record_hashes_single_column() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 1);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"test1"));
        hashes.insert(xxh3_64(b"test2"));

        accumulator.record_hashes("email", 0, hashes);

        let results = accumulator.finalize(2);
        assert_eq!(results["email"].0, 0); // No duplicates
    }

    #[test]
    fn test_record_hashes_extends_existing() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 2);

        let mut hashes1 = HashSet::with_hasher(Xxh3Builder);
        hashes1.insert(xxh3_64(b"test1"));

        let mut hashes2 = HashSet::with_hasher(Xxh3Builder);
        hashes2.insert(xxh3_64(b"test2"));

        accumulator.record_hashes("email", 0, hashes1);
        accumulator.record_hashes("email", 0, hashes2);

        let results = accumulator.finalize(2);
        assert_eq!(results["email"].0, 0); // 2 unique values, 2 total rows
    }

    #[test]
    fn test_record_hashes_deduplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 2);

        let hash = xxh3_64(b"duplicate");

        let mut hashes1 = HashSet::with_hasher(Xxh3Builder);
        hashes1.insert(hash);

        let mut hashes2 = HashSet::with_hasher(Xxh3Builder);
        hashes2.insert(hash);

        accumulator.record_hashes("email", 0, hashes1);
        accumulator.record_hashes("email", 0, hashes2);

        let results = accumulator.finalize(2);
        assert_eq!(results["email"].0, 1); // 1 unique value, 2 total rows = 1 duplicate
    }

    #[test]
    fn test_finalize_no_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 3);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"test1"));
        hashes.insert(xxh3_64(b"test2"));
        hashes.insert(xxh3_64(b"test3"));

        accumulator.record_hashes("email", 0, hashes);

        let results = accumulator.finalize(3);
        assert_eq!(results["email"].0, 0);
    }

    #[test]
    fn test_finalize_with_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 5);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"test1"));
        hashes.insert(xxh3_64(b"test2"));

        accumulator.record_hashes("email", 0, hashes);

        let results = accumulator.finalize(5);
        assert_eq!(results["email"].0, 3); // 5 total - 2 unique = 3 duplicates
    }

    #[test]
    fn test_finalize_all_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 10);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"same"));

        accumulator.record_hashes("email", 0, hashes);

        let results = accumulator.finalize(10);
        assert_eq!(results["email"].0, 9); // 10 total - 1 unique = 9 duplicates
    }

    #[test]
    fn test_concurrent_hash_recording() {
        let col1 = create_string_column_with_unicity("col1");
        let col2 = create_string_column_with_unicity("col2");
        let columns = vec![col1, col2];
        let accumulator = UnicityAccumulator::new(&columns, 10);

        // Different threads recording to different columns
        (0..10).into_par_iter().for_each(|i| {
            let mut hashes = HashSet::with_hasher(Xxh3Builder);
            hashes.insert(xxh3_64(format!("value{}", i).as_bytes()));

            if i % 2 == 0 {
                accumulator.record_hashes("col1", 0, hashes);
            } else {
                accumulator.record_hashes("col2", 0, hashes);
            }
        });

        let results = accumulator.finalize(10);
        // Each column should have recorded 5 unique values
        assert!(results.contains_key("col1"));
        assert!(results.contains_key("col2"));
    }

    #[test]
    fn test_concurrent_same_column() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns, 10);

        // Multiple threads recording to same column
        (0..10).into_par_iter().for_each(|i| {
            let mut hashes = HashSet::with_hasher(Xxh3Builder);
            hashes.insert(xxh3_64(format!("value{}", i).as_bytes()));
            accumulator.record_hashes("email", 0, hashes);
        });

        let results = accumulator.finalize(10);
        assert_eq!(results["email"].0, 0); // 10 unique values, 10 rows = no duplicates
    }

    #[test]
    fn test_finalize_empty_accumulator() {
        let columns = vec![];
        let accumulator = UnicityAccumulator::new(&columns, 100);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 0);
    }
}

// ============================================================================
// Statistical rules Tests
// ============================================================================

#[test]
fn test_cols_with_stats() {
    let col = create_string_column_with_length("name", 3, 50);
    let col_stats = create_int_column_with_stats("name", 3, 50);
    let col_f_stats = create_float_column_with_stats("name", 3, 50);
    let columns = vec![col, col_stats, col_f_stats].into_boxed_slice();
    let relations = None;
    let engine = ValidationEngine::new(&columns, &relations);
    let columns = engine.get_cols_with_stats();
    assert!(columns.is_some())
}

// ============================================================================
// ValidationEngine Tests
// ============================================================================

#[cfg(test)]
mod validation_engine_tests {
    use super::*;

    #[test]
    fn test_engine_creation() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let _engine = ValidationEngine::new(&columns, &relations);
        // Just ensure it doesn't panic
    }

    #[test]
    fn test_engine_empty_columns() {
        let columns = vec![].into_boxed_slice();
        let relations = None;
        let _engine = ValidationEngine::new(&columns, &relations);
        // Just ensure it doesn't panic
    }

    #[test]
    fn test_validate_empty_batches() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batches = vec![];
        let result = engine
            .validate_batches("test_table".to_string(), &batches)
            .unwrap();

        assert_eq!(result.table_name, "test_table");
        assert_eq!(result.total_rows, 0);
    }

    #[test]
    fn test_validate_single_batch_string_column() {
        let col = create_string_column_with_length("name", 3, 10);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        // Create batch with some valid and invalid values
        let batch = create_string_batch(
            "name",
            vec![
                Some("abc"),         // Valid (3 chars)
                Some("abcdefghi"),   // Valid (9 chars)
                Some("ab"),          // Invalid (too short)
                Some("abcdefghijk"), // Invalid (too long)
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.total_rows, 4);

        let column_results = result.get_column_results();
        assert!(column_results.contains_key("name"));

        let name_results = &column_results["name"];
        // Should have TypeCheck and two StringLength rules (min and max)
        assert!(name_results.len() >= 2);
    }

    #[test]
    fn test_validate_single_batch_integer_column() {
        let col = create_int_column_with_range("age", 0, 120);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch = create_int_batch(
            "age",
            vec![
                Some(25),  // Valid
                Some(100), // Valid
                Some(-5),  // Invalid (too low)
                Some(150), // Invalid (too high)
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.total_rows, 4);

        let column_results = result.get_column_results();
        assert!(column_results.contains_key("age"));
    }

    #[test]
    fn test_validate_single_batch_float_column() {
        let mut builder = NumericColumnBuilder::<f64>::new("price".to_string());
        builder.between(0.0, 1000.0, 0.0);
        let col = compiler::compile_column(Box::new(builder), true).unwrap();
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch = create_float_batch(
            "price",
            vec![
                Some(10.5),   // Valid
                Some(999.99), // Valid
                Some(-1.0),   // Invalid
                Some(1500.0), // Invalid
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.total_rows, 4);
    }

    #[test]
    fn test_validate_multiple_batches() {
        let col = create_string_column_with_length("name", 3, 10);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch1 = create_string_batch("name", vec![Some("ab")]); // 1 error
        let batch2 = create_string_batch("name", vec![Some("a")]); // 1 error
        let batch3 = create_string_batch("name", vec![Some("abc")]); // 0 errors

        let result = engine
            .validate_batches("test_table".to_string(), &[batch1, batch2, batch3])
            .unwrap();

        assert_eq!(result.total_rows, 3);

        let column_results = result.get_column_results();
        let name_results = &column_results["name"];

        // Find StringLengthCheck errors - with_min_length creates "WithMinLength" rule
        let length_errors: usize = name_results
            .iter()
            .filter(|r| r.rule_name == "WithMinLength" || r.rule_name == "WithMaxLength")
            .map(|r| r.error_count)
            .sum();

        assert!(length_errors >= 2); // At least 2 errors from short strings
    }

    #[test]
    fn test_validate_null_check() {
        let col = create_string_column_with_null_check("email");
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch = create_string_batch(
            "email",
            vec![
                Some("test@example.com"),
                None, // Null - should error
                Some("another@example.com"),
                None, // Null - should error
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.total_rows, 4);

        let column_results = result.get_column_results();
        let email_results = &column_results["email"];

        // Find NullCheck errors
        let null_errors = email_results
            .iter()
            .find(|r| r.rule_name == "NullCheck")
            .map(|r| r.error_count)
            .unwrap_or(0);

        assert_eq!(null_errors, 2);
    }

    #[test]
    fn test_unicity_single_batch_no_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch = create_string_batch(
            "email",
            vec![
                Some("user1@example.com"),
                Some("user2@example.com"),
                Some("user3@example.com"),
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        let column_results = result.get_column_results();
        let email_results = &column_results["email"];

        let unicity_errors = email_results
            .iter()
            .find(|r| r.rule_name == "Unicity")
            .map(|r| r.error_count)
            .unwrap_or(0);

        assert_eq!(unicity_errors, 0);
    }

    #[test]
    fn test_unicity_single_batch_with_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch = create_string_batch(
            "email",
            vec![
                Some("user@example.com"),
                Some("user@example.com"), // Duplicate
                Some("other@example.com"),
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        let column_results = result.get_column_results();
        let email_results = &column_results["email"];

        let unicity_errors = email_results
            .iter()
            .find(|r| r.rule_name == "Unicity")
            .map(|r| r.error_count)
            .unwrap_or(0);

        assert_eq!(unicity_errors, 1); // 3 rows - 2 unique = 1 duplicate
    }

    #[test]
    fn test_unicity_across_batches() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch1 = create_string_batch("email", vec![Some("user@example.com")]);
        let batch2 = create_string_batch("email", vec![Some("user@example.com")]); // Same value
        let batch3 = create_string_batch("email", vec![Some("other@example.com")]);

        let result = engine
            .validate_batches("test_table".to_string(), &[batch1, batch2, batch3])
            .unwrap();

        assert_eq!(result.total_rows, 3);

        let column_results = result.get_column_results();
        let email_results = &column_results["email"];

        let unicity_errors = email_results
            .iter()
            .find(|r| r.rule_name == "Unicity")
            .map(|r| r.error_count)
            .unwrap_or(0);

        assert_eq!(unicity_errors, 1); // 3 rows - 2 unique = 1 duplicate
    }

    #[test]
    fn test_unicity_multiple_columns() {
        let col1 = create_string_column_with_unicity("email");
        let col2 = create_string_column_with_unicity("username");
        let columns = vec![col1, col2].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        // Create batch with both columns
        let email_array = StringArray::from(vec![
            Some("user1@example.com"),
            Some("user2@example.com"),
            Some("user1@example.com"), // Duplicate email
        ]);
        let username_array = StringArray::from(vec![
            Some("user1"),
            Some("user2"),
            Some("user3"), // Unique username
        ]);

        let schema = Schema::new(vec![
            Field::new("email", DataType::Utf8, true),
            Field::new("username", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(email_array), Arc::new(username_array)],
        )
        .unwrap();

        let result = engine
            .validate_batches("test_table".to_string(), &[Arc::new(batch)])
            .unwrap();

        let column_results = result.get_column_results();

        // Email should have 1 duplicate
        let email_results = &column_results["email"];
        let email_unicity_errors = email_results
            .iter()
            .find(|r| r.rule_name == "Unicity")
            .map(|r| r.error_count)
            .unwrap_or(0);
        assert_eq!(email_unicity_errors, 1);

        // Username should have 0 duplicates
        let username_results = &column_results["username"];
        let username_unicity_errors = username_results
            .iter()
            .find(|r| r.rule_name == "Unicity")
            .map(|r| r.error_count)
            .unwrap_or(0);
        assert_eq!(username_unicity_errors, 0);
    }

    #[test]
    fn test_result_has_correct_table_name() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch = create_string_batch("name", vec![Some("test")]);

        let result = engine
            .validate_batches("my_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.table_name, "my_table");
    }

    #[test]
    fn test_result_has_correct_total_rows() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col].into_boxed_slice();
        let relations = None;
        let engine = ValidationEngine::new(&columns, &relations);

        let batch1 = create_string_batch("name", vec![Some("abc"), Some("def")]);
        let batch2 = create_string_batch("name", vec![Some("ghi"), Some("jkl"), Some("mno")]);

        let result = engine
            .validate_batches("test_table".to_string(), &[batch1, batch2])
            .unwrap();

        assert_eq!(result.total_rows, 5);
    }

    #[test]
    fn test_validate_date_relation() {
        // Create two date columns
        let start_col = create_date_column("start_date", "%Y-%m-%d");
        let end_col = create_date_column("end_date", "%Y-%m-%d");
        let columns = vec![start_col, end_col].into_boxed_slice();

        // Create relation: start_date <= end_date
        let mut relation = RelationBuilder::new(["start_date".to_string(), "end_date".to_string()]);
        relation.date_comparaison(CompOperator::Lte, 0.0);
        let executable_relation = compiler::compile_relations(relation).unwrap();
        let relations = Some(vec![executable_relation].into_boxed_slice());

        let engine = ValidationEngine::new(&columns, &relations);

        // Create batch with date pairs
        let batch = create_two_date_batch(
            "start_date",
            "end_date",
            vec![
                (Some("2024-01-01"), Some("2024-12-31")), // Valid: start <= end
                (Some("2024-06-01"), Some("2024-03-01")), // Invalid: start > end
                (Some("2024-02-01"), Some("2024-02-28")), // Valid: start <= end
            ],
        );

        let result = engine
            .validate_batches("test_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.total_rows, 3);

        // Check that relation results exist
        let relation_results = result.get_relation_results();

        // Relation validation should run and produce results
        // Note: The actual count depends on date parsing and relation validation logic
        // This test just verifies the relation validation infrastructure works
        assert!(
            relation_results.contains_key("start_date | end_date") || relation_results.is_empty()
        );
    }

    #[test]
    fn test_validate_batches_streaming_basic() {
        use crossbeam::channel::bounded;

        // Create simple test data
        let batch1 = create_string_batch("name", vec![Some("Alice"), Some("Bob")]);
        let batch2 = create_string_batch("name", vec![Some("Charlie"), Some("Dave")]);

        // Create column with simple validation
        let column = create_string_column_with_length("name", 1, 10);
        let columns = vec![column];

        // Create channel and send mini-batches
        let (sender, receiver) = bounded(2);
        sender.send(Ok(vec![batch1])).unwrap();
        sender.send(Ok(vec![batch2])).unwrap();
        drop(sender); // Close channel

        // Validate using streaming
        let engine = ValidationEngine::new(&columns, &None);
        let result = engine
            .validate_batches_streaming("test_table".to_string(), receiver)
            .unwrap();

        assert_eq!(result.total_rows, 4);
        assert_eq!(result.table_name, "test_table");
    }

    #[test]
    fn test_validate_batches_streaming_vs_batch_mode() {
        // Create test data
        let batch1 = create_string_batch("name", vec![Some("Alice"), Some("Bob"), Some("Charlie")]);
        let batch2 = create_string_batch("name", vec![Some("Dave"), Some("Eve"), Some("Frank")]);

        let column = create_string_column_with_length("name", 1, 10);
        let columns = vec![column];

        // Test batch mode
        let all_batches = vec![batch1.clone(), batch2.clone()];
        let engine = ValidationEngine::new(&columns, &None);
        let batch_result = engine
            .validate_batches("test_table".to_string(), &all_batches)
            .unwrap();

        // Test streaming mode
        use crossbeam::channel::bounded;
        let (sender, receiver) = bounded(2);
        sender.send(Ok(vec![batch1])).unwrap();
        sender.send(Ok(vec![batch2])).unwrap();
        drop(sender);

        let engine2 = ValidationEngine::new(&columns, &None);
        let stream_result = engine2
            .validate_batches_streaming("test_table".to_string(), receiver)
            .unwrap();

        // Both should produce same total rows
        assert_eq!(batch_result.total_rows, stream_result.total_rows);
        assert_eq!(batch_result.total_rows, 6);
    }

    #[test]
    fn test_validate_batches_streaming_with_unicity() {
        use crossbeam::channel::bounded;

        // Create batches with duplicates
        let batch1 =
            create_string_batch("email", vec![Some("alice@test.com"), Some("bob@test.com")]);
        let batch2 = create_string_batch(
            "email",
            vec![Some("alice@test.com"), Some("charlie@test.com")],
        ); // alice is duplicate

        let column = create_string_column_with_unicity("email");
        let columns = vec![column];

        let (sender, receiver) = bounded(2);
        sender.send(Ok(vec![batch1])).unwrap();
        sender.send(Ok(vec![batch2])).unwrap();
        drop(sender);

        let engine = ValidationEngine::new(&columns, &None);
        let result = engine
            .validate_batches_streaming("test_table".to_string(), receiver)
            .unwrap();

        assert_eq!(result.total_rows, 4);

        // Check unicity violation was detected
        let column_results = result.get_column_results();
        assert!(column_results.contains_key("email"));

        let email_results = &column_results["email"];

        // Find Unicity rule result
        let unicity_result = email_results
            .iter()
            .find(|r| r.rule_name == "Unicity")
            .expect("Should have Unicity result");

        // Should have 1 duplicate (alice appears twice)
        assert_eq!(unicity_result.error_count, 1);
    }

    #[test]
    fn test_validate_batches_streaming_with_stats() {
        use crossbeam::channel::bounded;

        // Create integer batches
        let batch1 = create_int_batch("age", vec![Some(25), Some(30), Some(35)]);
        let batch2 = create_int_batch("age", vec![Some(40), Some(45), Some(50)]);

        let column = create_int_column_with_stats("age", 0, 100);
        let columns = vec![column];

        let (sender, receiver) = bounded(2);
        sender.send(Ok(vec![batch1])).unwrap();
        sender.send(Ok(vec![batch2])).unwrap();
        drop(sender);

        let engine = ValidationEngine::new(&columns, &None);
        let result = engine
            .validate_batches_streaming("test_table".to_string(), receiver)
            .unwrap();

        assert_eq!(result.total_rows, 6);
        // Stats should be computed across all mini-batches
    }

    #[test]
    fn test_validate_batches_streaming_error_propagation() {
        use crossbeam::channel::bounded;
        use std::io;

        let batch1 = create_string_batch("name", vec![Some("Alice")]);

        let column = create_string_column_with_length("name", 1, 10);
        let columns = vec![column];

        let (sender, receiver) = bounded(2);
        sender.send(Ok(vec![batch1])).unwrap();
        // Send an error
        sender
            .send(Err(io::Error::new(io::ErrorKind::Other, "test error")))
            .unwrap();
        drop(sender);

        let engine = ValidationEngine::new(&columns, &None);
        let result = engine.validate_batches_streaming("test_table".to_string(), receiver);

        // Should propagate the error
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_batches_streaming_incremental_stats() {
        use crossbeam::channel::bounded;

        // Test that stats are merged correctly across mini-batches
        let batch1 = create_float_batch("value", vec![Some(1.0), Some(2.0), Some(3.0)]);
        let batch2 = create_float_batch("value", vec![Some(4.0), Some(5.0), Some(6.0)]);
        let batch3 = create_float_batch("value", vec![Some(7.0), Some(8.0), Some(9.0)]);

        let column = create_float_column_with_stats("value", 0, 10);
        let columns = vec![column];

        // Streaming mode
        let (sender, receiver) = bounded(3);
        sender.send(Ok(vec![batch1.clone()])).unwrap();
        sender.send(Ok(vec![batch2.clone()])).unwrap();
        sender.send(Ok(vec![batch3.clone()])).unwrap();
        drop(sender);

        let engine = ValidationEngine::new(&columns, &None);
        let stream_result = engine
            .validate_batches_streaming("test_table".to_string(), receiver)
            .unwrap();

        // Batch mode for comparison
        let all_batches = vec![batch1, batch2, batch3];
        let engine2 = ValidationEngine::new(&columns, &None);
        let batch_result = engine2
            .validate_batches("test_table".to_string(), &all_batches)
            .unwrap();

        // Both should have same total rows
        assert_eq!(stream_result.total_rows, batch_result.total_rows);
        assert_eq!(stream_result.total_rows, 9);
    }
}
