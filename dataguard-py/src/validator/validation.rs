use crate::columns;
use crate::reader::read_csv_parallel;
use crate::report::ValidationReport;
use crate::rules::core::Rule as RuleEnum;
use crate::rules::generic_rules::{TypeCheck, UnicityCheck};
use crate::rules::numeric_rules::{Monotonicity, NumericRule, Range};
use crate::rules::string_rules::{IsInCheck, RegexMatch, StringLengthCheck, StringRule};
use crate::utils::hasher::Xxh3Builder;
use arrow::array::{Array, PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use arrow_array::ArrowPrimitiveType;
use pyo3::{exceptions::PyIOError, prelude::*};
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use super::executable_column::ExecutableColumn;

#[cfg(feature = "python")]
#[pyclass(name = "Validator")]
pub struct Validator {
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
    pub fn new() -> Self {
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
    pub fn commit(&mut self, columns: Vec<columns::Column>) -> PyResult<()> {
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
                                    RuleEnum::StringMembers { members } => {
                                        Box::new(IsInCheck::new(members))
                                    }
                                    _ => {
                                        todo!()
                                    }
                                }
                            })
                            .collect();

                        // Return the constructed ExecutableColumn variant, wrapped in Some
                        // For unicity, the option should always be a Unicity RuleEnum so we do not
                        // match on it
                        Some(ExecutableColumn::String {
                            name: col.name.clone(),
                            rules: executable_rules,
                            type_check: TypeCheck::new(col.name, DataType::Utf8),
                            unicity: col.unicity.map(|_r| UnicityCheck {}),
                        })
                    }
                    "integer" => {
                        let executable_rules: Vec<Box<dyn NumericRule<Int64Type>>> = col
                            .rules
                            .into_iter()
                            .map(|r| -> Box<dyn NumericRule<Int64Type>> {
                                // Match on the RuleEnum from Python
                                match r {
                                    RuleEnum::NumericRange { min, max } => {
                                        Box::new(Range::<i64>::new(
                                            min.map(|v| v as i64),
                                            max.map(|v| v as i64),
                                        ))
                                    }
                                    RuleEnum::Monotonicity { asc } => {
                                        Box::new(Monotonicity::<i64>::new(asc))
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
                    "float" => {
                        let executable_rules: Vec<Box<dyn NumericRule<Float64Type>>> = col
                            .rules
                            .into_iter()
                            .map(|r| -> Box<dyn NumericRule<Float64Type>> {
                                match r {
                                    RuleEnum::NumericRange { min, max } => {
                                        Box::new(Range::<f64>::new(min, max))
                                    }
                                    RuleEnum::Monotonicity { asc } => {
                                        Box::new(Monotonicity::<f64>::new(asc))
                                    }
                                    _ => {
                                        todo!()
                                    }
                                }
                            })
                            .collect();

                        Some(ExecutableColumn::Float {
                            name: col.name.clone(),
                            rules: executable_rules,
                            type_check: TypeCheck::new(col.name, DataType::Float64),
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
    pub fn validate_csv(&mut self, path: &str, print_report: bool) -> PyResult<usize> {
        let start = Instant::now();
        let batches = read_csv_parallel(path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let read_duration = start.elapsed();
        eprintln!("CSV reading took {:?}", read_duration);
        let validation_start = Instant::now();

        let error_count = AtomicUsize::new(0);
        let report = ValidationReport::new();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        report.set_total_rows(total_rows);

        for executable_col in &self.executable_columns {
            match executable_col {
                ExecutableColumn::String {
                    name,
                    rules,
                    type_check,
                    unicity,
                } => self._validate_string_column(
                    &batches,
                    name,
                    rules,
                    type_check,
                    unicity,
                    &report,
                    &error_count,
                    total_rows,
                ),
                ExecutableColumn::Integer {
                    name,
                    rules,
                    type_check,
                } => self._validate_numeric_column::<Int64Type>(
                    &batches,
                    name,
                    rules,
                    type_check,
                    &report,
                    &error_count,
                ),
                ExecutableColumn::Float {
                    name,
                    rules,
                    type_check,
                } => self._validate_numeric_column::<Float64Type>(
                    &batches,
                    name,
                    rules,
                    type_check,
                    &report,
                    &error_count,
                ),
            }
        }
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
    pub fn get_rules(&self) -> PyResult<HashMap<String, Vec<String>>> {
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
                ExecutableColumn::Float { name, rules, .. } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
            }
        }
        Ok(result)
    }
}

impl Validator {
    fn _validate_string_column(
        &self,
        batches: &[Arc<RecordBatch>],
        name: &str,
        rules: &[Box<dyn StringRule>],
        type_check: &TypeCheck,
        unicity: &Option<UnicityCheck>,
        report: &ValidationReport,
        error_count: &AtomicUsize,
        total_rows: usize,
    ) {
        if let Some(uni_rule) = unicity {
            let global_hash = batches
                .par_iter()
                .map(|batch| {
                    let mut local_hash = HashSet::with_hasher(Xxh3Builder);
                    if let Ok(col_index) = batch.schema().index_of(name) {
                        let array = batch.column(col_index);
                        match type_check.validate(array.as_ref()) {
                            Ok((errors, casted_array)) => {
                                error_count.fetch_add(errors, Ordering::Relaxed);
                                report.record_result(name, type_check.name(), errors);

                                if let Some(string_array) =
                                    casted_array.as_any().downcast_ref::<StringArray>()
                                {
                                    local_hash = uni_rule.validate(string_array);
                                    for rule in rules {
                                        if let Ok(count) =
                                            rule.validate(string_array, name.to_string())
                                        {
                                            error_count.fetch_add(count, Ordering::Relaxed);
                                            report.record_result(name, rule.name(), count);
                                        }
                                    }
                                    local_hash
                                } else {
                                    local_hash
                                }
                            }
                            Err(_) => {
                                let count = array.len();
                                error_count.fetch_add(count, Ordering::Relaxed);
                                report.record_result(name, type_check.name(), count);
                                local_hash
                            }
                        }
                    } else {
                        local_hash
                    }
                })
                .reduce(
                    || HashSet::with_hasher(Xxh3Builder),
                    |mut set_a, set_b| {
                        set_a.extend(set_b.iter());
                        set_a
                    },
                );
            let duplicates = total_rows.saturating_sub(global_hash.len());
            error_count.fetch_add(duplicates, Ordering::Relaxed);
            report.record_result(name, uni_rule.name(), duplicates);
        } else {
            batches.par_iter().for_each(|batch| {
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
                                    if let Ok(count) = rule.validate(string_array, name.to_string())
                                    {
                                        error_count.fetch_add(count, Ordering::Relaxed);
                                        report.record_result(name, rule.name(), count);
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            let count = array.len();
                            error_count.fetch_add(count, Ordering::Relaxed);
                            report.record_result(name, type_check.name(), count);
                        }
                    }
                }
            });
        }
    }

    fn _validate_numeric_column<T>(
        &self,
        batches: &[Arc<RecordBatch>],
        name: &str,
        rules: &[Box<dyn NumericRule<T>>],
        type_check: &TypeCheck,
        report: &ValidationReport,
        error_count: &AtomicUsize,
    ) where
        T: ArrowPrimitiveType,
    {
        batches.par_iter().for_each(|batch| {
            if let Ok(col_index) = batch.schema().index_of(name) {
                let array = batch.column(col_index);
                match type_check.validate(array.as_ref()) {
                    Ok((errors, casted_array)) => {
                        error_count.fetch_add(errors, Ordering::Relaxed);
                        report.record_result(name, type_check.name(), errors);

                        if let Some(concrete_array) =
                            casted_array.as_any().downcast_ref::<PrimitiveArray<T>>()
                        {
                            for rule in rules {
                                if let Ok(count) = rule.validate(concrete_array, name.to_string()) {
                                    error_count.fetch_add(count, Ordering::Relaxed);
                                    report.record_result(name, rule.name(), count);
                                }
                            }
                        }
                    }
                    Err(_) => {
                        let count = array.len();
                        error_count.fetch_add(count, Ordering::Relaxed);
                        report.record_result(name, type_check.name(), count);
                    }
                }
            }
        });
    }
}
