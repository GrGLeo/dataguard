pub mod columns;
pub mod errors;
pub mod reader;
pub mod report;
pub mod rules;
pub mod types;

use crate::columns::{string_column::StringColumnBuilder, Column};
use crate::reader::read_csv_parallel;
use crate::report::ValidationReport;
use crate::rules::core::Rule as RuleEnum;
use crate::rules::logic::{RegexMatch, StringLengthCheck, StringRule};
use arrow::array::StringArray;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// An internal enum to hold the compiled, logic-bearing validation rules for each column type.
enum ExecutableColumn {
    String {
        name: String,
        rules: Vec<Box<dyn StringRule>>,
    },
    // Future column types like Integer would be another variant here
}

#[cfg(feature = "python")]
#[pyclass(name = "Validator")]
struct Validator {
    executable_columns: Vec<ExecutableColumn>,
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
                                }
                            })
                            .collect();

                        // Return the constructed ExecutableColumn variant, wrapped in Some
                        Some(ExecutableColumn::String {
                            name: col.name,
                            rules: executable_rules,
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
        let batches =
            read_csv_parallel(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
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
                    ExecutableColumn::String { name, rules } => {
                        if let Ok(col_index) = batch.schema().index_of(name) {
                            let array = batch.column(col_index);

                            // KEY OPTIMIZATION: Cast ONCE here.
                            if let Some(string_array) = array.as_any().downcast_ref::<StringArray>()
                            {
                                // Loop through the typed StringRules
                                for rule in rules {
                                    // Pass the already-casted array to each rule
                                    if let Ok(count) = rule.validate(string_array, name.clone()) {
                                        if count > 0 {
                                            error_count.fetch_add(count, Ordering::Relaxed);
                                            report.record_result(name, rule.name(), count);
                                        }
                                    }
                                }
                            } else {
                                // Handle type mismatch error: all rows are considered errors.
                                let count = array.len();
                                error_count.fetch_add(count, Ordering::Relaxed);
                                report.record_result(name, "TypeMismatch", count);
                            }
                        }
                    } // Add other ExecutableColumn variants here
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
                ExecutableColumn::String { name, rules } => {
                    let rule_names = rules.iter().map(|r| r.name().to_string()).collect();
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

/// DataGuard: A high-performance CSV validation library.
#[cfg(feature = "python")]
#[pyo3::pymodule]
fn dataguard(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Validator>()?;
    m.add_class::<Column>()?;
    m.add_class::<StringColumnBuilder>()?;
    m.add_function(wrap_pyfunction!(string_column, m)?)?;
    Ok(())
}
