pub mod columns;
pub mod errors;
pub mod reader;
pub mod report;
pub mod rules;
pub mod types;

#[cfg(feature = "python")]
use crate::columns::integer_column::IntegerColumnBuilder;
use crate::columns::{Column, string_column::StringColumnBuilder};
use crate::reader::read_csv_parallel;
use crate::report::ValidationReport;
use crate::rules::core::Rule as RuleEnum;
use crate::rules::logic::{
    IntegerRange, IntegerRule, RegexMatch, StringLengthCheck, StringRule, TypeCheck,
};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::DataType;
use pyo3::{exceptions::PyIOError, prelude::*};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// An internal enum to hold the compiled, logic-bearing validation rules for each column type.
enum ExecutableColumn {
    String {
        name: String,
        rules: Vec<Box<dyn StringRule>>,
        type_check: TypeCheck,
    },
    // Future column types like Integer would be another variant here
    Integer {
        name: String,
        rules: Vec<Box<dyn IntegerRule>>,
        type_check: TypeCheck,
    },
}

#[cfg(feature = "python")]
#[pyclass(name = "Validator")]
struct Validator {
    executable_columns: Vec<ExecutableColumn>,
}

#[cfg(feature = "python")]
impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl Validator {
    /// Create a new Validator instance.
    #[new]
    fn new() -> Self {
        Self {
            executable_columns: Vec::new(),
        }
    }

    /// Compiles and commits a list of column configurations to the validator.
    /// This method transforms the simple `Column` data objects into executable
    /// validation rules.
    ///
    /// Args:
    ///     columns (list[Column]): A list of configured Column objects from Python.
    fn commit(&mut self, columns: Vec<Column>) -> PyResult<()> {
        // We use filter_map to iterate, transform, and filter out any unhandled types in one pass.
        self.executable_columns = columns
            .into_iter()
            .filter_map(|col| {
                // The match statement is now an expression that returns a value.
                match col.column_type.as_str() {
                    "string" => {
                        let executable_rules: Vec<Box<dyn StringRule>> = col
                            .rules
                            .into_iter()
                            .map(|r| -> Box<dyn StringRule> {
                                // Match on the RuleEnum from Python
                                match r {
                                    RuleEnum::StringLength { min, max } => {
                                        Box::new(StringLengthCheck::new(min, max))
                                    }
                                    RuleEnum::StringRegex { pattern, flag } => {
                                        Box::new(RegexMatch::new(pattern, flag))
                                    }
                                    _ => {
                                        todo!()
                                    }
                                }
                            })
                            .collect();

                        // Return the constructed ExecutableColumn variant, wrapped in Some
                        Some(ExecutableColumn::String {
                            name: col.name.clone(),
                            rules: executable_rules,
                            type_check: TypeCheck::new(col.name, DataType::Utf8),
                        })
                    }
                    "integer" => {
                        let executable_rules: Vec<Box<dyn IntegerRule>> = col
                            .rules
                            .into_iter()
                            .map(|r| -> Box<dyn IntegerRule> {
                                // Match on the RuleEnum from Python
                                match r {
                                    RuleEnum::IntegerRange { min, max } => {
                                        Box::new(IntegerRange::new(min, max))
                                    }
                                    _ => {
                                        todo!()
                                    }
                                }
                            })
                            .collect();

                        // Return the constructed ExecutableColumn variant, wrapped in Some
                        Some(ExecutableColumn::Integer {
                            name: col.name.clone(),
                            rules: executable_rules,
                            type_check: TypeCheck::new(col.name, DataType::Int64),
                        })
                    }
                    // Add other column types here in the future
                    _ => None, // Ignore unknown column types
                }
            })
            .collect();
        Ok(())
    }

    /// Validate a CSV file against the committed rules.
    ///
    /// Args:
    ///     path (str): Path to the CSV file to validate.
    ///     print_report (bool): Whether to print the validation report.
    ///
    /// Returns:
    ///     int: The number of validation errors found.
    fn validate_csv(&mut self, path: &str, print_report: bool) -> PyResult<usize> {
        let start = Instant::now();
        let batches = read_csv_parallel(path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let read_duration = start.elapsed();
        eprintln!("CSV reading took {:?}", read_duration);
        let validation_start = Instant::now();

        let error_count = AtomicUsize::new(0);
        let report = ValidationReport::new();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        report.set_total_rows(total_rows);

        batches.par_iter().for_each(|batch| {
            for executable_col in &self.executable_columns {
                // KEY CHANGE: Match on the ExecutableColumn enum
                match executable_col {
                    ExecutableColumn::String {
                        name,
                        rules,
                        type_check,
                    } => {
                        if let Ok(col_index) = batch.schema().index_of(name) {
                            let array = batch.column(col_index);

                            match type_check.validate(array.as_ref()) {
                                Ok((errors, casted_array)) => {
                                    error_count.fetch_add(errors, Ordering::Relaxed);
                                    report.record_result(name, type_check.name(), errors);

                                    if let Some(string_array) =
                                        casted_array.as_any().downcast_ref::<StringArray>()
                                    {
                                        for rule in rules {
                                            if let Ok(count) =
                                                rule.validate(string_array, name.clone())
                                            {
                                                error_count.fetch_add(count, Ordering::Relaxed);
                                                report.record_result(name, rule.name(), count);
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    // TypeCheck validation itself failed, meaning the cast was not possible.
                                    // All rows are considered errors.
                                    let count = array.len();
                                    error_count.fetch_add(count, Ordering::Relaxed);
                                    report.record_result(name, type_check.name(), count);
                                }
                            }
                        }
                    }
                    ExecutableColumn::Integer {
                        name,
                        rules,
                        type_check,
                    } => {
                        if let Ok(col_index) = batch.schema().index_of(name) {
                            let array = batch.column(col_index);

                            match type_check.validate(array.as_ref()) {
                                Ok((errors, casted_array)) => {
                                    error_count.fetch_add(errors, Ordering::Relaxed);
                                    report.record_result(name, type_check.name(), errors);

                                    if let Some(integer_array) =
                                        casted_array.as_any().downcast_ref::<Int64Array>()
                                    {
                                        for rule in rules {
                                            if let Ok(count) =
                                                rule.validate(integer_array, name.clone())
                                            {
                                                error_count.fetch_add(count, Ordering::Relaxed);
                                                report.record_result(name, rule.name(), count);
                                            }
                                        }
                                    } else {
                                        println!("Failed downcast");
                                    }
                                }
                                Err(_) => {
                                    // TypeCheck validation itself failed, meaning the cast was not possible.
                                    // All rows are considered errors.
                                    let count = array.len();
                                    error_count.fetch_add(count, Ordering::Relaxed);
                                    report.record_result(name, type_check.name(), count);
                                }
                            }
                        }
                    }
                }
            }
        });
        let validation_duration = validation_start.elapsed();
        eprintln!("Validation took {:?}", validation_duration);

        if print_report {
            println!("{}", report.generate_report());
        }

        Ok(error_count.load(Ordering::Relaxed))
    }

    /// Get a summary of the configured columns and their rules.
    ///
    /// Returns:
    ///     dict: A dictionary of the configured columns.
    fn get_rules(&self) -> PyResult<HashMap<String, Vec<String>>> {
        let mut result = HashMap::new();
        for column in &self.executable_columns {
            // KEY CHANGE: Match to access the data inside the enum variant
            match column {
                ExecutableColumn::String { name, rules, .. } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
                ExecutableColumn::Integer { name, rules, .. } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
            }
        }
        Ok(result)
    }
}

/// Creates a builder for defining rules on a string column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     StringColumnBuilder: A builder object for chaining rules.
#[cfg(feature = "python")]
#[pyfunction]
fn string_column(name: String) -> PyResult<StringColumnBuilder> {
    Ok(StringColumnBuilder::new(name))
}

/// Creates a builder for defining rules on a integer column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     IntegerColumnBuilder: A builder object for chaining rules.
#[cfg(feature = "python")]
#[pyfunction]
fn integer_column(name: String) -> PyResult<IntegerColumnBuilder> {
    Ok(IntegerColumnBuilder::new(name))
}

/// DataGuard: A high-performance CSV validation library.
#[cfg(feature = "python")]
#[pyo3::pymodule]
fn dataguard(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Validator>()?;
    m.add_class::<Column>()?;
    m.add_class::<StringColumnBuilder>()?;
    m.add_function(wrap_pyfunction!(string_column, m)?)?;
    m.add_function(wrap_pyfunction!(integer_column, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_builder_commit_and_get_rules() {
        // 1. Create builders and build Column DTOs
        let col1 = string_column("col1".to_string())
            .unwrap()
            .with_length_between(Some(1), Some(10)) // Changed
            .unwrap()
            .build();

        let col2 = string_column("col2".to_string())
            .unwrap()
            .with_regex("^[a-z]+$", None) // Changed
            .unwrap()
            .build();

        // 2. Create a validator and commit the columns
        let mut validator = Validator::new();
        validator.commit(vec![col1, col2]).unwrap();

        // 3. Check the internal state via get_rules()
        let rules = validator.get_rules().unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "StringLengthCheck".to_string()]
        );
        assert_eq!(
            rules.get("col2").unwrap(),
            &vec!["TypeCheck".to_string(), "RegexMatch".to_string()]
        );

        // 4. Check internal executable types (not directly testable from outside,
        // but get_rules success implies the transformation worked)
        assert_eq!(validator.executable_columns.len(), 2);
        match &validator.executable_columns[0] {
            ExecutableColumn::String {
                name,
                rules,
                type_check: _,
            } => {
                assert_eq!(name, "col1");
                assert_eq!(rules.len(), 1);
                assert_eq!(rules[0].name(), "StringLengthCheck");
            }
        }
        match &validator.executable_columns[1] {
            ExecutableColumn::String {
                name,
                rules,
                type_check: _,
            } => {
                assert_eq!(name, "col2");
                assert_eq!(rules.len(), 1);
                assert_eq!(rules[0].name(), "RegexMatch");
            }
        }
    }

    #[test]
    fn test_validate_csv_end_to_end() {
        // 1. Setup: Create a temporary CSV file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "product_id,description,price").unwrap();
        writeln!(file, "p1,short,10.0").unwrap(); // desc length fail (<6)
        writeln!(file, "p2,a good description,20.0").unwrap(); // ok
        writeln!(file, "p3,invalid-char!,30.0").unwrap(); // desc regex fail (! is invalid)
        writeln!(file, "p4,another good one,40.0").unwrap(); // ok
        writeln!(file, "p5,,50.0").unwrap(); // desc length fail ("" < 6)
        writeln!(file, "p6,12345,60.0").unwrap(); // desc regex fail (numbers not allowed)

        // 2. Create column rules
        let desc_col = string_column("description".to_string())
            .unwrap()
            .with_regex("^[a-z ]+$", None) // Changed
            .unwrap()
            .with_min_length(6) // Changed
            .unwrap()
            .build();

        // 3. Commit to validator
        let mut validator = Validator::new();
        validator.commit(vec![desc_col]).unwrap();

        // 4. Run validation
        let error_count = validator
            .validate_csv(file_path.to_str().unwrap(), false)
            .unwrap();

        // 5. Assert results
        // Expected errors:
        // - "short": pass | fail
        // - "a good description": pass | pass
        // - "invalid-char!": fail | pass
        // - "": fail | fail
        // - "12345": fail | fail
        assert_eq!(error_count, 6);

        // The tempdir will be automatically cleaned up when `dir` goes out of scope.
    }
}
