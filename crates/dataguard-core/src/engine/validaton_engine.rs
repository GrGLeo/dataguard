use super::accumulator::ResultAccumulator;
use arrow::datatypes::{Float64Type, Int64Type};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arrow_array::{Array, ArrowNumericType, PrimitiveArray, RecordBatch, StringArray};
use rayon::prelude::*;

use crate::{
    engine::unicity_accumulator::UnicityAccumulator,
    rules::{NullCheck, NumericRule, StringRule, TypeCheck, UnicityCheck},
    validator::ExecutableColumn,
    RuleError, ValidationResult,
};

/// ValidationEngine - executes validation rules on Arrow RecordBatches.
///
/// Independent of data source - works with any system that produces Arrow batches.
/// CsvTable reads CSV → batches, ParquetTable reads parquet → batches,
/// both use the same engine for validation.
pub struct ValidationEngine<'a> {
    columns: &'a Vec<ExecutableColumn>,
}

impl<'a> ValidationEngine<'a> {
    pub fn new(columns: &'a Vec<ExecutableColumn>) -> Self {
        Self { columns }
    }

    /// Validate batches and produce a validation result.
    /// Returns aggregated results suitable for reporting.
    pub fn validate_batches(
        &self,
        table_name: String,
        batches: &[Arc<RecordBatch>],
    ) -> Result<ValidationResult, RuleError> {
        let error_count = AtomicUsize::new(0);
        let report = ResultAccumulator::new();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        report.set_total_rows(total_rows);

        let unicity_accumulators = UnicityAccumulator::new(self.columns);

        batches.par_iter().for_each(|batch| {
            for executable_col in self.columns {
                match executable_col {
                    ExecutableColumn::String {
                        name,
                        rules,
                        type_check,
                        unicity_check,
                        null_check,
                    } => {
                        if let Ok(col_index) = batch.schema().index_of(name) {
                            let array = batch.column(col_index);
                            validate_string_column(
                                name,
                                rules,
                                type_check,
                                unicity_check,
                                null_check,
                                array,
                                &error_count,
                                &report,
                                &unicity_accumulators,
                            );
                        }
                    }
                    ExecutableColumn::Integer {
                        name,
                        rules,
                        type_check,
                        unicity_check,
                        null_check,
                    } => {
                        if let Ok(col_index) = batch.schema().index_of(name) {
                            let array = batch.column(col_index);
                            validate_numeric_column::<Int64Type>(
                                name,
                                rules,
                                type_check,
                                unicity_check,
                                null_check,
                                array,
                                &error_count,
                                &report,
                                &unicity_accumulators,
                            );
                        }
                    }
                    ExecutableColumn::Float {
                        name,
                        rules,
                        type_check,
                        unicity_check,
                        null_check,
                    } => {
                        if let Ok(col_index) = batch.schema().index_of(name) {
                            let array = batch.column(col_index);
                            validate_numeric_column::<Float64Type>(
                                name,
                                rules,
                                type_check,
                                unicity_check,
                                null_check,
                                array,
                                &error_count,
                                &report,
                                &unicity_accumulators,
                            );
                        }
                    }
                }
            }
        });

        // We need to calculate the unicity errors now
        // We unwrap all lock should have been clearer from the earlier loop
        let unicity_errors = unicity_accumulators.finalize(total_rows);
        for (column_name, unicity_error) in unicity_errors {
            error_count.fetch_add(unicity_error, Ordering::Relaxed);
            report.record_result(&column_name, "Unicity", unicity_error);
        }

        // We create the validation result for report formatting
        let column_results = report.to_results();
        let total_errors = error_count.load(Ordering::Relaxed);
        let mut results = ValidationResult::new(table_name.clone(), total_rows);
        results.add_column_results(column_results);

        // TODO: not sure about that
        if total_errors > 0 {
            results.set_failed("Too much errors found".to_string());
        }
        Ok(results)
    }
}

/// Validate null check and record results
fn validate_null_check(
    null_check: &Option<NullCheck>,
    array: &dyn Array,
    column_name: &str,
    report: &ResultAccumulator,
) {
    if let Some(null_rule) = null_check {
        let null_count = null_rule.validate(array);
        report.record_result(column_name, null_rule.name(), null_count);
    }
}

/// Record validation result and update error count
fn record_validation_result(
    column_name: &str,
    rule_name: &'static str,
    error_count_value: usize,
    error_count: &AtomicUsize,
    report: &ResultAccumulator,
) {
    error_count.fetch_add(error_count_value, Ordering::Relaxed);
    report.record_result(column_name, rule_name, error_count_value);
}

/// Record type check error when casting fails completely
fn record_type_check_error(
    array_len: usize,
    column_name: &str,
    type_check_name: &'static str,
    error_count: &AtomicUsize,
    report: &ResultAccumulator,
) {
    error_count.fetch_add(array_len, Ordering::Relaxed);
    report.record_result(column_name, type_check_name, array_len);
}

fn validate_string_column(
    name: &str,
    rules: &[Box<dyn StringRule>],
    type_check: &TypeCheck,
    unicity_check: &Option<UnicityCheck>,
    null_check: &Option<NullCheck>,
    array: &dyn Array,
    error_count: &AtomicUsize,
    report: &ResultAccumulator,
    unicity_accumulators: &UnicityAccumulator,
) {
    // Run null check if present
    validate_null_check(null_check, array, name, report);

    // We always run type check
    match type_check.validate(array) {
        Ok((errors, casted_array)) => {
            record_validation_result(name, type_check.name(), errors, error_count, report);

            // We downcast once the array
            if let Some(string_array) = casted_array.as_any().downcast_ref::<StringArray>() {
                // We run all domain level rules
                for rule in rules {
                    if let Ok(count) = rule.validate(string_array, name.to_string()) {
                        record_validation_result(name, rule.name(), count, error_count, report);
                    }
                }
                // If we have a unicity rule in place, update the global hashset
                if let Some(unicity_rule) = unicity_check {
                    let local_hash = unicity_rule.validate_str(string_array);
                    unicity_accumulators.record_hashes(name, local_hash);
                }
            }
        }
        Err(_) => {
            record_type_check_error(array.len(), name, type_check.name(), error_count, report);
        }
    }
}

/// Validate a numeric column (generic over Int64Type and Float64Type)
fn validate_numeric_column<T>(
    name: &str,
    rules: &[Box<dyn NumericRule<T>>],
    type_check: &TypeCheck,
    unicity_check: &Option<UnicityCheck>,
    null_check: &Option<NullCheck>,
    array: &dyn Array,
    error_count: &AtomicUsize,
    report: &ResultAccumulator,
    unicity_accumulators: &UnicityAccumulator,
) where
    T: ArrowNumericType,
{
    // Run null check if present
    validate_null_check(null_check, array, name, report);

    // We always run type check
    match type_check.validate(array) {
        Ok((errors, casted_array)) => {
            record_validation_result(name, type_check.name(), errors, error_count, report);

            // We downcast once the array
            if let Some(numeric_array) = casted_array.as_any().downcast_ref::<PrimitiveArray<T>>() {
                // We run all domain level rules
                for rule in rules {
                    if let Ok(count) = rule.validate(numeric_array, name.to_string()) {
                        record_validation_result(name, rule.name(), count, error_count, report);
                    }
                }
                // If we have a unicity rule in place, update the global hashset
                if let Some(unicity_rule) = unicity_check {
                    let local_hash = unicity_rule.validate_numeric(numeric_array);
                    unicity_accumulators.record_hashes(name, local_hash);
                }
            }
        }
        Err(_) => {
            record_type_check_error(array.len(), name, type_check.name(), error_count, report);
        }
    }
}
