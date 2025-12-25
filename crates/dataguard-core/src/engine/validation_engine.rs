use super::accumulator::ResultAccumulator;
use arrow::datatypes::{Float64Type, Int64Type};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use arrow_array::{Array, ArrowNumericType, PrimitiveArray, RecordBatch, StringArray};
use rayon::prelude::*;

use crate::{
    engine::unicity_accumulator::UnicityAccumulator,
    rules::{
        date::{DateRule, DateTypeCheck},
        NullCheck, NumericRule, StringRule, TypeCheck, UnicityCheck,
    },
    validator::{ExecutableColumn, ExecutableRelation},
    RuleError, ValidationResult,
};

/// ValidationEngine - executes validation rules on Arrow RecordBatches.
///
/// Independent of data source - works with any system that produces Arrow batches.
/// CsvTable reads CSV → batches, ParquetTable reads parquet → batches,
/// both use the same engine for validation.
pub struct ValidationEngine<'a> {
    columns: &'a [ExecutableColumn],
    relations: &'a Option<Box<[ExecutableRelation]>>,
}

impl<'a> ValidationEngine<'a> {
    pub fn new(
        columns: &'a [ExecutableColumn],
        relations: &'a Option<Box<[ExecutableRelation]>>,
    ) -> Self {
        Self { columns, relations }
    }

    /// Validate batches and produce a validation result.
    /// Returns aggregated results suitable for reporting.
    pub fn validate_batches(
        &self,
        table_name: String,
        batches: &[Arc<RecordBatch>],
    ) -> Result<ValidationResult, RuleError> {
        let error_counter = AtomicUsize::new(0);
        let report = ResultAccumulator::new();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        report.set_total_rows(total_rows);

        let unicity_accumulators = UnicityAccumulator::new(self.columns, total_rows);

        batches.par_iter().for_each(|batch| {
            // We keep in memory a reference to the casted array
            let mut array_ref: HashMap<String, Arc<dyn Array>> = HashMap::new();
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
                            let _ = validate_string_column(
                                name,
                                rules,
                                type_check,
                                unicity_check,
                                null_check,
                                array,
                                &error_counter,
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
                            let _ = validate_numeric_column::<Int64Type>(
                                name,
                                rules,
                                type_check,
                                unicity_check,
                                null_check,
                                array,
                                &error_counter,
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
                            let _ = validate_numeric_column::<Float64Type>(
                                name,
                                rules,
                                type_check,
                                unicity_check,
                                null_check,
                                array,
                                &error_counter,
                                &report,
                                &unicity_accumulators,
                            );
                        }
                    }
                    ExecutableColumn::Date {
                        name,
                        rules,
                        type_check,
                        unicity_check,
                        null_check,
                    } => {
                        let Ok(col_index) = batch.schema().index_of(name) else {
                            continue;
                        };
                        let array = batch.column(col_index);
                        if let Ok(casted_array) = validate_date_column(
                            name,
                            rules,
                            type_check,
                            unicity_check,
                            null_check,
                            array,
                            &error_counter,
                            &report,
                            &unicity_accumulators,
                        ) {
                            array_ref.insert(name.clone(), casted_array);
                        }
                    }
                }
            }
            if let Some(relations) = self.relations {
                for executable_relation in relations {
                    // Since the array could not be added in case of type cast failure
                    // We ensure that both key exist before running the validation
                    if array_ref.contains_key(&executable_relation.names[0])
                        && array_ref.contains_key(&executable_relation.names[1])
                    {
                        validate_relation(executable_relation, &array_ref, &error_counter, &report);
                    }
                }
            }
        });

        // We need to calculate the unicity errors now
        // We unwrap all lock should have been clearer from the earlier loop
        let unicity_errors = unicity_accumulators.finalize(total_rows);
        // TODO: for now we use a temp data 0. while making string work
        for (column_name, unicity_error) in unicity_errors {
            error_counter.fetch_add(unicity_error, Ordering::Relaxed);
            report.record_column_result(&column_name, "Unicity".to_string(), 0., unicity_error);
        }

        // We create the validation result for report formatting
        let (column_values, column_results, relation_result) = report.to_results();
        let mut results = ValidationResult::new(table_name.clone(), total_rows);
        results.add_columns_values(column_values);
        results.add_column_results(column_results);
        results.add_relation_results(relation_result);

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
        report.record_column_result(
            column_name,
            null_rule.name(),
            null_rule.get_threshold(),
            null_count,
        );
    }
}

/// Record validation result and update error count
fn record_validation_result(
    column_name: &str,
    rule_name: String,
    error_count_value: usize,
    error_counter: &AtomicUsize,
    threshold: f64,
    report: &ResultAccumulator,
    is_col: bool,
) {
    error_counter.fetch_add(error_count_value, Ordering::Relaxed);
    if is_col {
        report.record_column_result(column_name, rule_name, threshold, error_count_value);
    } else {
        report.record_relation_result(column_name, rule_name, threshold, error_count_value);
    }
}

/// Record type check error when casting fails completely
fn record_type_check_error(
    array_len: usize,
    column_name: &str,
    type_check_name: String,
    threshold: f64,
    error_counter: &AtomicUsize,
    report: &ResultAccumulator,
) {
    error_counter.fetch_add(array_len, Ordering::Relaxed);
    report.record_column_result(column_name, type_check_name, threshold, array_len);
}

fn validate_string_column(
    name: &str,
    rules: &[Box<dyn StringRule>],
    type_check: &Option<TypeCheck>,
    unicity_check: &Option<UnicityCheck>,
    null_check: &Option<NullCheck>,
    array: &dyn Array,
    error_counter: &AtomicUsize,
    report: &ResultAccumulator,
    unicity_accumulators: &UnicityAccumulator,
) -> Result<(), RuleError> {
    let array_values = array.len() - array.null_count();
    report.record_valid_values(name, array_values);
    // Run null check if present
    validate_null_check(null_check, array, name, report);

    // we only run a type check if the table is a CsvTable
    if let Some(type_rule) = type_check {
        match type_rule.validate(array) {
            Ok((errors, casted_array)) => {
                record_validation_result(
                    name,
                    type_rule.name(),
                    errors,
                    error_counter,
                    0.,
                    report,
                    true,
                );
                if errors == array_values {
                    // We return early in case of a full invalid initial data type
                    return Err(RuleError::TypeCastFailed);
                }

                // Safety: casted_array is StringArray in Ok path
                let string_array = casted_array.as_any().downcast_ref::<StringArray>().unwrap();
                // We run all domain level rules
                for rule in rules {
                    if let Ok(count) = rule.validate(string_array, name.to_string()) {
                        record_validation_result(
                            name,
                            rule.name(),
                            count,
                            error_counter,
                            rule.get_treshold(),
                            report,
                            true,
                        );
                    }
                }
                // If we have a unicity rule in place, update the global hashset
                if let Some(unicity_rule) = unicity_check {
                    let (null_count, local_hash) = unicity_rule.validate_str(string_array);
                    unicity_accumulators.record_hashes(name, null_count, local_hash);
                }
                return Ok(());
            }
            Err(e) => {
                record_type_check_error(
                    array.len(),
                    name,
                    type_rule.name(),
                    0.,
                    error_counter,
                    report,
                );
                return Err(e);
            }
        }
    }
    // HACK: for now we return an err, since we always have a typecheck rule
    Err(RuleError::TypeCastError(
        name.to_string(),
        "NumericType".to_string(),
    ))
}

/// Validate a numeric column (generic over Int64Type and Float64Type)
fn validate_numeric_column<T: ArrowNumericType>(
    name: &str,
    rules: &[Box<dyn NumericRule<T>>],
    type_check: &Option<TypeCheck>,
    unicity_check: &Option<UnicityCheck>,
    null_check: &Option<NullCheck>,
    array: &dyn Array,
    error_counter: &AtomicUsize,
    report: &ResultAccumulator,
    unicity_accumulators: &UnicityAccumulator,
) -> Result<(), RuleError> {
    let array_values = array.len() - array.null_count();
    report.record_valid_values(name, array_values);
    // Run null check if present
    validate_null_check(null_check, array, name, report);

    // we only run a type check if the table is a CsvTable
    if let Some(type_rule) = type_check {
        match type_rule.validate(array) {
            Ok((errors, casted_array)) => {
                record_validation_result(
                    name,
                    type_rule.name(),
                    errors,
                    error_counter,
                    0.,
                    report,
                    true,
                );
                if errors == array_values {
                    // We return early in case of a full invalid initial data type
                    return Err(RuleError::TypeCastFailed);
                }

                // Safety: casted_array is StringArray in Ok path
                let numeric_array = casted_array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap();
                // We run all domain level rules
                for rule in rules {
                    if let Ok(count) = rule.validate(numeric_array, name.to_string()) {
                        record_validation_result(
                            name,
                            rule.name(),
                            count,
                            error_counter,
                            rule.get_threshold(),
                            report,
                            true,
                        );
                    }
                }
                // If we have a unicity rule in place, update the global hashset
                if let Some(unicity_rule) = unicity_check {
                    let (null_count, local_hash) = unicity_rule.validate_numeric(numeric_array);
                    unicity_accumulators.record_hashes(name, null_count, local_hash);
                }
                return Ok(());
            }
            Err(e) => {
                record_type_check_error(
                    array.len(),
                    name,
                    type_rule.name(),
                    0.,
                    error_counter,
                    report,
                );
                return Err(e);
            }
        }
    }
    // HACK: for now we return an err, since we always have a typecheck rule
    Err(RuleError::TypeCastError(
        name.to_string(),
        "NumericType".to_string(),
    ))
}

pub fn validate_date_column(
    name: &str,
    rules: &[Box<dyn DateRule>],
    type_check: &Option<DateTypeCheck>,
    unicity_check: &Option<UnicityCheck>,
    null_check: &Option<NullCheck>,
    array: &dyn Array,
    error_counter: &AtomicUsize,
    report: &ResultAccumulator,
    unicity_accumulators: &UnicityAccumulator,
) -> Result<Arc<dyn Array>, RuleError> {
    let array_values = array.len() - array.null_count();
    report.record_valid_values(name, array_values);
    // Run null check if present
    validate_null_check(null_check, array, name, report);

    // we only run a type check if the table is a CsvTable
    if let Some(type_rule) = type_check {
        match type_rule.validate(array) {
            Ok((errors, date_array)) => {
                record_validation_result(
                    name,
                    type_rule.name(),
                    errors,
                    error_counter,
                    0.,
                    report,
                    true,
                );
                if errors == array_values {
                    // We return early in case of a full invalid initial data type
                    return Err(RuleError::TypeCastFailed);
                }

                // We run all domain level rules
                for rule in rules {
                    if let Ok(count) = rule.validate(&date_array, name.to_string()) {
                        record_validation_result(
                            name,
                            rule.name(),
                            count,
                            error_counter,
                            rule.get_threshold(),
                            report,
                            true,
                        );
                    }
                }
                // If we have a unicity rule in place, update the global hashset
                if let Some(unicity_rule) = unicity_check {
                    let (null_count, local_hash) = unicity_rule.validate_date(&date_array);
                    unicity_accumulators.record_hashes(name, null_count, local_hash);
                }
                return Ok(Arc::new(date_array));
            }
            Err(_) => {
                record_type_check_error(
                    array.len(),
                    name,
                    type_rule.name(),
                    0.,
                    error_counter,
                    report,
                );
                return Err(RuleError::TypeCastError(
                    name.to_string(),
                    "Date32Array".to_string(),
                ));
            }
        }
    }
    // HACK: for now we return an err, since we always have a typecheck rule
    Err(RuleError::TypeCastError(
        name.to_string(),
        "Date32Array".to_string(),
    ))
}

fn validate_relation(
    executable_relation: &ExecutableRelation,
    array_ref: &HashMap<String, Arc<dyn Array>>,
    error_counter: &AtomicUsize,
    report: &ResultAccumulator,
) {
    let lhs_name = executable_relation.names[0].as_str();
    let rhs_name = executable_relation.names[1].as_str();
    // We can safely unwrap as both keys are check before calling the function
    let lsh = array_ref.get(lhs_name).unwrap();
    let rhs = array_ref.get(rhs_name).unwrap();
    for rule in &executable_relation.rules {
        if let Ok(count) = rule.validate(lsh, rhs, [lhs_name, rhs_name]) {
            record_validation_result(
                format!("{} | {}", lhs_name, rhs_name).as_str(),
                rule.name(),
                count,
                error_counter,
                rule.get_threshold(),
                report,
                false,
            );
        }
    }
}
