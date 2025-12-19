use crate::column::{ColumnBuilder, ColumnRule, ColumnType};
use crate::errors::RuleError;
use crate::reader::read_csv_parallel;
use crate::report::ValidationReport;
use crate::rules::generic::{TypeCheck, UnicityCheck};
use crate::rules::numeric::{Monotonicity, NumericRule, Range};
use crate::rules::string::{IsInCheck, RegexMatch, StringLengthCheck, StringRule};
use crate::rules::NullCheck;
use crate::tables::Table;
use crate::utils::hasher::Xxh3Builder;
use crate::validator::ExecutableColumn;
use crate::ValidationResult;
use arrow::array::{Array, PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow_array::ArrowNumericType;
use num_traits::{Num, NumCast};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct CsvTable {
    path: String,
    table_name: String,
    executable_columns: Vec<ExecutableColumn>,
}

impl CsvTable {
    /// Create a new Validator instance
    pub fn new(path: String, table_name: String) -> Result<Self, RuleError> {
        Ok(Self {
            path,
            table_name,
            executable_columns: Vec::new(),
        })
    }
}

impl Table for CsvTable {
    /// Commit column configurations and compile them into executable rules
    fn commit(&mut self, columns: Vec<Box<dyn ColumnBuilder>>) -> Result<(), RuleError> {
        self.executable_columns = columns
            .into_iter()
            .map(|col| self.compile_column_builder(col))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }

    /// Validate a CSV file against the committed rules
    fn validate(&mut self) -> Result<ValidationResult, RuleError> {
        let needed_cols: Vec<String> = self
            .executable_columns
            .iter()
            .map(|v| v.get_name())
            .collect();
        let batches = read_csv_parallel(self.path.as_str(), needed_cols)?;

        let error_count = AtomicUsize::new(0);
        let report = ValidationReport::new();

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        report.set_total_rows(total_rows);

        let mut unicity_accumulators: HashMap<String, Arc<Mutex<HashSet<u64, Xxh3Builder>>>> =
            HashMap::new();

        for column in &self.executable_columns {
            if column.has_unicity() {
                unicity_accumulators.insert(
                    column.get_name(),
                    Arc::new(Mutex::new(HashSet::with_hasher(Xxh3Builder))),
                );
            }
        }

        batches.par_iter().for_each(|batch| {
            for executable_col in &self.executable_columns {
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
                            Self::validate_string_column(
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
                            Self::validate_numeric_column::<Int64Type>(
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
                            Self::validate_numeric_column::<Float64Type>(
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
        for (c, h) in unicity_accumulators {
            let i = h.as_ref().lock().unwrap().len();
            let errors = total_rows - i;
            error_count.fetch_add(errors, Ordering::Relaxed);
            report.record_result(c.as_str(), "Unicity", errors);
        }

        // We create the validation result for report formatting
        let column_results = report.to_results();
        let total_errors = error_count.load(Ordering::Relaxed);
        let mut results = ValidationResult::new(self.table_name.clone(), total_rows);
        results.add_column_results(column_results);

        // TODO: not sure about that
        if total_errors > 0 {
            results.set_failed("Too much errors found".to_string());
        }
        Ok(results)
    }

    /// Get a summary of configured rules
    fn get_rules(&self) -> HashMap<String, Vec<String>> {
        let mut result = HashMap::new();
        for column in &self.executable_columns {
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
        result
    }

    // Private helper methods
    fn compile_column_builder(
        &self,
        builder: Box<dyn ColumnBuilder>,
    ) -> Result<ExecutableColumn, RuleError> {
        match builder.column_type() {
            ColumnType::String => {
                let mut executable_rules: Vec<Box<dyn StringRule>> = Vec::new();
                let mut unicity_check = None;
                let mut null_check = None;

                for rule in builder.rules() {
                    match rule {
                        ColumnRule::StringLength { min, max } => {
                            executable_rules.push(Box::new(StringLengthCheck::new(*min, *max)));
                        }
                        ColumnRule::StringRegex { pattern, flags } => {
                            executable_rules
                                .push(Box::new(RegexMatch::new(pattern.clone(), flags.clone())));
                        }
                        ColumnRule::StringMembers { members } => {
                            executable_rules.push(Box::new(IsInCheck::new(members.to_vec())));
                        }
                        ColumnRule::Unicity => {
                            unicity_check = Some(UnicityCheck::new());
                        }
                        ColumnRule::NullCheck => {
                            null_check = Some(NullCheck::new());
                        }
                        _ => {
                            return Err(RuleError::ValidationError(format!(
                                "Invalid rule {:?} for String column '{}'",
                                rule,
                                builder.name()
                            )))
                        }
                    }
                }

                Ok(ExecutableColumn::String {
                    name: builder.name().to_string(),
                    rules: executable_rules,
                    type_check: TypeCheck::new(builder.name().to_string(), DataType::Utf8),
                    unicity_check,
                    null_check,
                })
            }
            ColumnType::Integer => {
                let res = compile_numeric_rules(builder.rules(), builder.name());
                match res {
                    Ok((executable_rules, unicity_check, null_check)) => {
                        Ok(ExecutableColumn::Integer {
                            name: builder.name().to_string(),
                            rules: executable_rules,
                            type_check: TypeCheck::new(builder.name().to_string(), DataType::Int64),
                            unicity_check,
                            null_check,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
            ColumnType::Float => {
                let res = compile_numeric_rules(builder.rules(), builder.name());
                match res {
                    Ok((executable_rules, unicity_check, null_check)) => {
                        Ok(ExecutableColumn::Float {
                            name: builder.name().to_string(),
                            rules: executable_rules,
                            type_check: TypeCheck::new(
                                builder.name().to_string(),
                                DataType::Float64,
                            ),
                            unicity_check,
                            null_check,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }
}

impl CsvTable {
    /// Validate null check and record results
    fn validate_null_check(
        null_check: &Option<NullCheck>,
        array: &dyn Array,
        column_name: &str,
        report: &ValidationReport,
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
        report: &ValidationReport,
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
        report: &ValidationReport,
    ) {
        error_count.fetch_add(array_len, Ordering::Relaxed);
        report.record_result(column_name, type_check_name, array_len);
    }

    /// Update the global unicity accumulator with local hashes
    fn update_unicity_accumulator(
        local_hash: HashSet<u64, Xxh3Builder>,
        column_name: &str,
        unicity_accumulators: &HashMap<String, Arc<Mutex<HashSet<u64, Xxh3Builder>>>>,
    ) {
        unicity_accumulators
            .get(column_name)
            .unwrap()
            .lock()
            .unwrap()
            .extend(local_hash);
    }

    /// Validate a string column
    fn validate_string_column(
        name: &str,
        rules: &[Box<dyn StringRule>],
        type_check: &TypeCheck,
        unicity_check: &Option<UnicityCheck>,
        null_check: &Option<NullCheck>,
        array: &dyn Array,
        error_count: &AtomicUsize,
        report: &ValidationReport,
        unicity_accumulators: &HashMap<String, Arc<Mutex<HashSet<u64, Xxh3Builder>>>>,
    ) {
        // Run null check if present
        Self::validate_null_check(null_check, array, name, report);

        // We always run type check
        match type_check.validate(array) {
            Ok((errors, casted_array)) => {
                Self::record_validation_result(
                    name,
                    type_check.name(),
                    errors,
                    error_count,
                    report,
                );

                // We downcast once the array
                if let Some(string_array) = casted_array.as_any().downcast_ref::<StringArray>() {
                    // We run all domain level rules
                    for rule in rules {
                        if let Ok(count) = rule.validate(string_array, name.to_string()) {
                            Self::record_validation_result(
                                name,
                                rule.name(),
                                count,
                                error_count,
                                report,
                            );
                        }
                    }
                    // If we have a unicity rule in place, update the global hashset
                    if let Some(unicity_rule) = unicity_check {
                        let local_hash = unicity_rule.validate_str(string_array);
                        Self::update_unicity_accumulator(local_hash, name, unicity_accumulators);
                    }
                }
            }
            Err(_) => {
                Self::record_type_check_error(
                    array.len(),
                    name,
                    type_check.name(),
                    error_count,
                    report,
                );
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
        report: &ValidationReport,
        unicity_accumulators: &HashMap<String, Arc<Mutex<HashSet<u64, Xxh3Builder>>>>,
    ) where
        T: ArrowNumericType,
    {
        // Run null check if present
        Self::validate_null_check(null_check, array, name, report);

        // We always run type check
        match type_check.validate(array) {
            Ok((errors, casted_array)) => {
                Self::record_validation_result(
                    name,
                    type_check.name(),
                    errors,
                    error_count,
                    report,
                );

                // We downcast once the array
                if let Some(numeric_array) =
                    casted_array.as_any().downcast_ref::<PrimitiveArray<T>>()
                {
                    // We run all domain level rules
                    for rule in rules {
                        if let Ok(count) = rule.validate(numeric_array, name.to_string()) {
                            Self::record_validation_result(
                                name,
                                rule.name(),
                                count,
                                error_count,
                                report,
                            );
                        }
                    }
                    // If we have a unicity rule in place, update the global hashset
                    if let Some(unicity_rule) = unicity_check {
                        let local_hash = unicity_rule.validate_numeric(numeric_array);
                        Self::update_unicity_accumulator(local_hash, name, unicity_accumulators);
                    }
                }
            }
            Err(_) => {
                Self::record_type_check_error(
                    array.len(),
                    name,
                    type_check.name(),
                    error_count,
                    report,
                );
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn compile_numeric_rules<N, A>(
    rules: &[ColumnRule],
    column_name: &str,
) -> Result<
    (
        Vec<Box<dyn NumericRule<A>>>,
        Option<UnicityCheck>,
        Option<NullCheck>,
    ),
    RuleError,
>
where
    N: Num + PartialOrd + Copy + Debug + Send + Sync + NumCast + 'static,
    A: ArrowNumericType<Native = N>,
{
    let mut unicity = None;
    let mut null_rule = None;
    let mut executable_rules: Vec<Box<dyn NumericRule<A>>> = Vec::new();
    for rule in rules {
        match rule {
            ColumnRule::NumericRange { min, max } => {
                let min_conv = min.and_then(|v| N::from(v));
                let max_conv = max.and_then(|v| N::from(v));
                executable_rules.push(Box::new(Range::<N>::new(min_conv, max_conv)));
            }
            ColumnRule::Monotonicity { ascending } => {
                executable_rules.push(Box::new(Monotonicity::<N>::new(*ascending)));
            }
            ColumnRule::NullCheck => null_rule = Some(NullCheck::new()),
            ColumnRule::Unicity => {
                unicity = Some(UnicityCheck::new());
            }
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "Invalid rule {:?} for numeric column '{}'",
                    rule, column_name
                )))
            }
        }
    }
    Ok((executable_rules, unicity, null_rule))
}
