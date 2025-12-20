//! Tests for the validation engine module.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::column::{NumericColumnBuilder, StringColumnBuilder};
use crate::compiler;
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
    builder.is_unique();
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for a string column with length constraints.
fn create_string_column_with_length(name: &str, min: usize, max: usize) -> ExecutableColumn {
    let mut builder = StringColumnBuilder::new(name.to_string());
    builder.with_min_length(min).with_max_length(max);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for an integer column with range constraints.
fn create_int_column_with_range(name: &str, min: i64, max: i64) -> ExecutableColumn {
    let mut builder = NumericColumnBuilder::<i64>::new(name.to_string());
    builder.between(min, max);
    compiler::compile_column(Box::new(builder), true).unwrap()
}

/// Create an ExecutableColumn for a string column with null check.
fn create_string_column_with_null_check(name: &str) -> ExecutableColumn {
    let mut builder = StringColumnBuilder::new(name.to_string());
    builder.is_not_null();
    compiler::compile_column(Box::new(builder), true).unwrap()
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
        let results = accumulator.to_results();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_record_single_result() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_result("column1", "rule1", 5);

        let results = accumulator.to_results();
        assert_eq!(results.len(), 1);
        assert!(results.contains_key("column1"));

        let column_results = &results["column1"];
        assert_eq!(column_results.len(), 1);
        assert_eq!(column_results[0].rule_name, "rule1");
        assert_eq!(column_results[0].error_count, 5);
        assert_eq!(column_results[0].error_percentage, 5.0);
    }

    #[test]
    fn test_record_multiple_results_same_column() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_result("column1", "rule1", 5);
        accumulator.record_result("column1", "rule2", 10);

        let results = accumulator.to_results();
        assert_eq!(results.len(), 1);

        let column_results = &results["column1"];
        assert_eq!(column_results.len(), 2);
    }

    #[test]
    fn test_record_multiple_columns() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_result("column1", "rule1", 5);
        accumulator.record_result("column2", "rule1", 10);

        let results = accumulator.to_results();
        assert_eq!(results.len(), 2);
        assert!(results.contains_key("column1"));
        assert!(results.contains_key("column2"));
    }

    #[test]
    fn test_percentage_calculation() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(200);
        accumulator.record_result("column1", "rule1", 50);

        let results = accumulator.to_results();
        let column_results = &results["column1"];
        assert_eq!(column_results[0].error_percentage, 25.0);
    }

    #[test]
    fn test_percentage_zero_total_rows() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(0);
        accumulator.record_result("column1", "rule1", 5);

        let results = accumulator.to_results();
        let column_results = &results["column1"];
        assert_eq!(column_results[0].error_percentage, 0.0);
    }

    #[test]
    fn test_accumulates_errors() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_result("column1", "rule1", 5);
        accumulator.record_result("column1", "rule1", 3);
        accumulator.record_result("column1", "rule1", 2);

        let results = accumulator.to_results();
        let column_results = &results["column1"];
        assert_eq!(column_results[0].error_count, 10);
    }

    #[test]
    fn test_results_sorted_by_column() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(100);
        accumulator.record_result("zebra", "rule1", 1);
        accumulator.record_result("apple", "rule1", 1);
        accumulator.record_result("banana", "rule1", 1);

        let results = accumulator.to_results();
        let keys: Vec<_> = results.keys().cloned().collect();
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
            accumulator.record_result(&format!("column{}", i), "rule1", 1);
        });

        let results = accumulator.to_results();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_concurrent_same_column_rule() {
        let accumulator = ResultAccumulator::new();
        accumulator.set_total_rows(1000);

        // Multiple threads recording to same (column, rule)
        (0..100).into_par_iter().for_each(|_| {
            accumulator.record_result("column1", "rule1", 1);
        });

        let results = accumulator.to_results();
        let column_results = &results["column1"];
        assert_eq!(column_results[0].error_count, 100);
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

        let accumulator = UnicityAccumulator::new(&columns);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_new_creates_for_unicity_columns() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];

        let accumulator = UnicityAccumulator::new(&columns);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 1);
        assert!(results.contains_key("email"));
    }

    #[test]
    fn test_new_multiple_unicity_columns() {
        let col1 = create_string_column_with_unicity("email");
        let col2 = create_string_column_with_unicity("username");
        let columns = vec![col1, col2];

        let accumulator = UnicityAccumulator::new(&columns);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_record_hashes_single_column() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"test1"));
        hashes.insert(xxh3_64(b"test2"));

        accumulator.record_hashes("email", hashes);

        let results = accumulator.finalize(2);
        assert_eq!(results["email"], 0); // No duplicates
    }

    #[test]
    fn test_record_hashes_extends_existing() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns);

        let mut hashes1 = HashSet::with_hasher(Xxh3Builder);
        hashes1.insert(xxh3_64(b"test1"));

        let mut hashes2 = HashSet::with_hasher(Xxh3Builder);
        hashes2.insert(xxh3_64(b"test2"));

        accumulator.record_hashes("email", hashes1);
        accumulator.record_hashes("email", hashes2);

        let results = accumulator.finalize(2);
        assert_eq!(results["email"], 0); // 2 unique values, 2 total rows
    }

    #[test]
    fn test_record_hashes_deduplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns);

        let hash = xxh3_64(b"duplicate");

        let mut hashes1 = HashSet::with_hasher(Xxh3Builder);
        hashes1.insert(hash);

        let mut hashes2 = HashSet::with_hasher(Xxh3Builder);
        hashes2.insert(hash);

        accumulator.record_hashes("email", hashes1);
        accumulator.record_hashes("email", hashes2);

        let results = accumulator.finalize(2);
        assert_eq!(results["email"], 1); // 1 unique value, 2 total rows = 1 duplicate
    }

    #[test]
    fn test_finalize_no_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"test1"));
        hashes.insert(xxh3_64(b"test2"));
        hashes.insert(xxh3_64(b"test3"));

        accumulator.record_hashes("email", hashes);

        let results = accumulator.finalize(3);
        assert_eq!(results["email"], 0);
    }

    #[test]
    fn test_finalize_with_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"test1"));
        hashes.insert(xxh3_64(b"test2"));

        accumulator.record_hashes("email", hashes);

        let results = accumulator.finalize(5);
        assert_eq!(results["email"], 3); // 5 total - 2 unique = 3 duplicates
    }

    #[test]
    fn test_finalize_all_duplicates() {
        let col = create_string_column_with_unicity("email");
        let columns = vec![col];
        let accumulator = UnicityAccumulator::new(&columns);

        let mut hashes = HashSet::with_hasher(Xxh3Builder);
        hashes.insert(xxh3_64(b"same"));

        accumulator.record_hashes("email", hashes);

        let results = accumulator.finalize(10);
        assert_eq!(results["email"], 9); // 10 total - 1 unique = 9 duplicates
    }

    #[test]
    fn test_concurrent_hash_recording() {
        let col1 = create_string_column_with_unicity("col1");
        let col2 = create_string_column_with_unicity("col2");
        let columns = vec![col1, col2];
        let accumulator = UnicityAccumulator::new(&columns);

        // Different threads recording to different columns
        (0..10).into_par_iter().for_each(|i| {
            let mut hashes = HashSet::with_hasher(Xxh3Builder);
            hashes.insert(xxh3_64(format!("value{}", i).as_bytes()));

            if i % 2 == 0 {
                accumulator.record_hashes("col1", hashes);
            } else {
                accumulator.record_hashes("col2", hashes);
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
        let accumulator = UnicityAccumulator::new(&columns);

        // Multiple threads recording to same column
        (0..10).into_par_iter().for_each(|i| {
            let mut hashes = HashSet::with_hasher(Xxh3Builder);
            hashes.insert(xxh3_64(format!("value{}", i).as_bytes()));
            accumulator.record_hashes("email", hashes);
        });

        let results = accumulator.finalize(10);
        assert_eq!(results["email"], 0); // 10 unique values, 10 rows = no duplicates
    }

    #[test]
    fn test_finalize_empty_accumulator() {
        let columns = vec![];
        let accumulator = UnicityAccumulator::new(&columns);
        let results = accumulator.finalize(100);
        assert_eq!(results.len(), 0);
    }
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
        let columns = vec![col];
        let _engine = ValidationEngine::new(&columns);
        // Just ensure it doesn't panic
    }

    #[test]
    fn test_engine_empty_columns() {
        let columns = vec![];
        let _engine = ValidationEngine::new(&columns);
        // Just ensure it doesn't panic
    }

    #[test]
    fn test_validate_empty_batches() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        builder.between(0.0, 1000.0);
        let col = compiler::compile_column(Box::new(builder), true).unwrap();
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

        let batch1 = create_string_batch("name", vec![Some("ab")]); // 1 error
        let batch2 = create_string_batch("name", vec![Some("a")]); // 1 error
        let batch3 = create_string_batch("name", vec![Some("abc")]); // 0 errors

        let result = engine
            .validate_batches("test_table".to_string(), &[batch1, batch2, batch3])
            .unwrap();

        assert_eq!(result.total_rows, 3);

        let column_results = result.get_column_results();
        let name_results = &column_results["name"];

        // Find StringLengthCheck errors
        let length_errors: usize = name_results
            .iter()
            .filter(|r| r.rule_name == "StringLengthCheck")
            .map(|r| r.error_count)
            .sum();

        assert!(length_errors >= 2); // At least 2 errors from short strings
    }

    #[test]
    fn test_validate_null_check() {
        let col = create_string_column_with_null_check("email");
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col1, col2];
        let engine = ValidationEngine::new(&columns);

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
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

        let batch = create_string_batch("name", vec![Some("test")]);

        let result = engine
            .validate_batches("my_table".to_string(), &[batch])
            .unwrap();

        assert_eq!(result.table_name, "my_table");
    }

    #[test]
    fn test_result_has_correct_total_rows() {
        let col = create_string_column_with_length("name", 3, 50);
        let columns = vec![col];
        let engine = ValidationEngine::new(&columns);

        let batch1 = create_string_batch("name", vec![Some("abc"), Some("def")]);
        let batch2 = create_string_batch("name", vec![Some("ghi"), Some("jkl"), Some("mno")]);

        let result = engine
            .validate_batches("test_table".to_string(), &[batch1, batch2])
            .unwrap();

        assert_eq!(result.total_rows, 5);
    }
}
