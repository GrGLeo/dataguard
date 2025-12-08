use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use arrow_array::{ArrowPrimitiveType, RecordBatch};

use crate::{
    column::ColumnBuilder,
    report::ValidationReport,
    rules::{NumericRule, StringRule, TypeCheck, UnicityCheck},
    validator::ExecutableColumn,
    RuleError,
};

pub mod csv_table;

pub trait Table {
    fn validate(&mut self) -> Result<(), RuleError>;
    fn commit(&mut self, columns: Vec<Box<dyn ColumnBuilder>>) -> Result<(), RuleError>;
    fn get_rules(&self) -> HashMap<String, Vec<String>>;
    fn compile_column_builder(
        &self,
        builder: Box<dyn ColumnBuilder>,
    ) -> Result<ExecutableColumn, RuleError>;
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
    );
    fn validate_numeric_column<T: ArrowPrimitiveType>(
        &self,
        batches: &[Arc<RecordBatch>],
        name: &str,
        rules: &[Box<dyn NumericRule<T>>],
        type_check: &TypeCheck,
        report: &ValidationReport,
        error_count: &AtomicUsize,
    );
}
