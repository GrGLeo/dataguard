use crate::column::{ColumnBuilder, ColumnRule, ColumnType};
use crate::errors::RuleError;
use crate::reader::read_csv_parallel;
use crate::report::{ReportMode, ValidationReport};
use crate::rules::generic::{TypeCheck, UnicityCheck};
use crate::rules::numeric::{Monotonicity, NumericRule, Range};
use crate::rules::string::{IsInCheck, RegexMatch, StringLengthCheck, StringRule};
use crate::tables::Table;
use crate::utils::hasher::Xxh3Builder;
use crate::validator::ExecutableColumn;
use arrow::array::{Array, PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use arrow_array::ArrowPrimitiveType;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct CsvTable {
    path: String,
    output: ReportMode,
    executable_columns: Vec<ExecutableColumn>,
}

impl CsvTable {
    /// Create a new Validator instance
    pub fn new(path: String, output: String) -> Result<Self, RuleError> {
        let mode = match output.to_lowercase().as_str() {
            "stdout" => ReportMode::StdOut,
            "json" => ReportMode::Json,
            "csv" => ReportMode::Csv,
            _ => return Err(RuleError::UnknownReportMode(output)),
        };
        Ok(Self {
            path,
            output: mode,
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
    fn validate(&mut self) -> Result<(), RuleError> {
        let needed_cols: Vec<String> = self
            .executable_columns
            .iter()
            .map(|v| v.get_name())
            .collect();
        let batches = read_csv_parallel(self.path.as_str(), needed_cols)?;

        let error_count = AtomicUsize::new(0);
        let report = ValidationReport::new(&self.output);

        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        report.set_total_rows(total_rows);

        for executable_col in &self.executable_columns {
            match executable_col {
                ExecutableColumn::String {
                    name,
                    rules,
                    type_check,
                    unicity,
                } => self.validate_string_column(
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
                } => self.validate_numeric_column::<Int64Type>(
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
                } => self.validate_numeric_column::<Float64Type>(
                    &batches,
                    name,
                    rules,
                    type_check,
                    &report,
                    &error_count,
                ),
            }
        }

        let output = report.generate_report(&self.output);
        match self.output {
            ReportMode::StdOut => println!("{}", output),
            ReportMode::Json => println!("{}", output),
            ReportMode::Csv => println!("{}", output),
        };

        // TODO: not sure about that
        if error_count.load(Ordering::Relaxed) > 0 {
            return Err(RuleError::ValidationError(
                "too much errors found".to_string(),
            ));
        }
        Ok(())
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
                let mut unicity = None;

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
                            unicity = Some(UnicityCheck::new());
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
                    unicity,
                })
            }
            ColumnType::Integer => {
                let mut executable_rules: Vec<Box<dyn NumericRule<Int64Type>>> = Vec::new();

                for rule in builder.rules() {
                    match rule {
                        ColumnRule::NumericRange { min, max } => {
                            executable_rules.push(Box::new(Range::<i64>::new(
                                min.map(|v| v as i64),
                                max.map(|v| v as i64),
                            )));
                        }
                        ColumnRule::Monotonicity { ascending } => {
                            executable_rules.push(Box::new(Monotonicity::<i64>::new(*ascending)));
                        }
                        _ => {
                            return Err(RuleError::ValidationError(format!(
                                "Invalid rule {:?} for Integer column '{}'",
                                rule,
                                builder.name()
                            )))
                        }
                    }
                }

                Ok(ExecutableColumn::Integer {
                    name: builder.name().to_string(),
                    rules: executable_rules,
                    type_check: TypeCheck::new(builder.name().to_string(), DataType::Int64),
                })
            }
            ColumnType::Float => {
                let mut executable_rules: Vec<Box<dyn NumericRule<Float64Type>>> = Vec::new();

                for rule in builder.rules() {
                    match rule {
                        ColumnRule::NumericRange { min, max } => {
                            executable_rules.push(Box::new(Range::<f64>::new(*min, *max)));
                        }
                        ColumnRule::Monotonicity { ascending } => {
                            executable_rules.push(Box::new(Monotonicity::<f64>::new(*ascending)));
                        }
                        _ => {
                            return Err(RuleError::ValidationError(format!(
                                "Invalid rule {:?} for Float column '{}'",
                                rule,
                                builder.name()
                            )))
                        }
                    }
                }

                Ok(ExecutableColumn::Float {
                    name: builder.name().to_string(),
                    rules: executable_rules,
                    type_check: TypeCheck::new(builder.name().to_string(), DataType::Float64),
                })
            }
        }
    }

    fn validate_string_column(
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

    fn validate_numeric_column<T: ArrowPrimitiveType>(
        &self,
        batches: &[Arc<RecordBatch>],
        name: &str,
        rules: &[Box<dyn NumericRule<T>>],
        type_check: &TypeCheck,
        report: &ValidationReport,
        error_count: &AtomicUsize,
    ) {
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
