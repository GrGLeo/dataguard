pub mod column_builder;
pub mod errors;
pub mod reader;
pub mod rules;
pub mod types;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

#[cfg(feature = "python")]
use pyo3::{exceptions::PyIOError, prelude::*};
use rayon::prelude::*;
#[cfg(feature = "python")]
use crate::column_builder::ColumnBuilder;
use crate::{reader::read_csv_parallel, types::RuleMap};

#[cfg(feature = "python")]
#[pyclass]
struct Validator {
    rules: Arc<Mutex<RuleMap>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl Validator {
    /// Create a new Validator instance for CSV data validation.
    #[new]
    fn new() -> Self {
        Self {
            rules: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a new column rule builder for the specified column.
    ///
    /// Args:
    ///     column_name (str): The name of the column to add rules for.
    ///
    /// Returns:
    ///     ColumnBuilder: A builder object to add validation rules for the column.
    fn add_column_rule(&self, column_name: &str) -> PyResult<ColumnBuilder> {
        Ok(ColumnBuilder::new(
            column_name.to_string(),
            Arc::clone(&self.rules),
        ))
    }

    /// Validate a CSV file against the defined rules.
    ///
    /// Args:
    ///     path (str): Path to the CSV file to validate.
    ///
    /// Returns:
    ///     int: The number of validation errors found.
    fn validate_csv(&mut self, path: &str) -> PyResult<usize> {
        let start = Instant::now();
        if let Ok(batches) = read_csv_parallel(path) {
            let read_duration = start.elapsed();
            eprintln!("CSV reading took {:?}", read_duration);
            let validation_start = Instant::now();
            let validation_rules = self.rules.lock().unwrap();
            let error_count = AtomicUsize::new(0);
            batches.par_iter().for_each(|batch| {
                for (colname, rules) in validation_rules.iter() {
                    if let Ok(col_index) = batch.schema().index_of(colname) {
                        let array = batch.column(col_index);
                        for rule in rules {
                            let res = rule.validate(array);
                            if let Ok(count) = res {
                                let _ = error_count.fetch_add(count, Ordering::Relaxed);
                            }
                        }
                    }
                }
            });
            let validation_duration = validation_start.elapsed();
            eprintln!("Validation took {:?}", validation_duration);
            Ok(error_count.load(Ordering::Relaxed))
        } else {
            Err(PyErr::new::<PyIOError, _>("Failed to load CSV"))
        }
    }

    /// Get a dictionary of all defined validation rules.
    ///
    /// Returns:
    ///     dict: A dictionary where keys are column names and values are lists of rule names.
    fn get_rules(&self) -> PyResult<HashMap<String, Vec<String>>> {
        let rules = self.rules.lock().unwrap();
        let mut result = HashMap::new();
        for (column, rule_list) in rules.iter() {
            let names: Vec<String> = rule_list.iter().map(|r| r.name().to_string()).collect();
            result.insert(column.clone(), names);
        }
        Ok(result)
    }
}

/// DataGuard: A high-performance CSV validation library.
/// Provides tools for defining validation rules and validating CSV files in parallel.
#[cfg(feature = "python")]
#[pyo3::pymodule]
mod dataguard {
    use pyo3::prelude::*;

    #[pymodule_export]
    use super::Validator;

    /// Calculate the sum of two numbers and return it as a string.
    ///
    /// Args:
    ///     a (int): First number to add.
    ///     b (int): Second number to add.
    ///
    /// Returns:
    ///     str: The sum of a and b as a string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}

#[cfg(all(test, feature = "python"))]
mod tests {
    use super::*;
    use crate::rules::TypeCheck;
    use arrow::datatypes::DataType;

    #[test]
    fn test_get_rules_empty() {
        let validator = Validator::new();
        let rules = validator.get_rules().unwrap();
        assert!(rules.is_empty());
    }

    #[test]
    fn test_get_rules_with_rules() {
        let validator = Validator::new();
        {
            let mut rules = validator.rules.lock().unwrap();
            rules.insert(
                "name".to_string(),
                vec![
                    Box::new(TypeCheck::new("name".to_string(), DataType::Utf8)),
                ],
            );
            rules.insert(
                "age".to_string(),
                vec![Box::new(TypeCheck::new("age".to_string(), DataType::Int64))],
            );
        }
        let rules_dict = validator.get_rules().unwrap();
        assert_eq!(rules_dict.len(), 2);
        assert_eq!(rules_dict["name"], vec!["TypeCheck"]);
        assert_eq!(rules_dict["age"], vec!["TypeCheck"]);
    }

    #[test]
    fn test_add_column_rule() {
        let validator = Validator::new();
        let builder = validator.add_column_rule("test_column").unwrap();
        assert_eq!(builder.column, "test_column");
    }
}
