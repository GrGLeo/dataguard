use std::collections::HashMap;

use crate::{column::ColumnBuilder, RuleError, ValidationResult};

pub mod csv_table;

pub trait Table {
    fn validate(&mut self) -> Result<ValidationResult, RuleError>;
    fn commit(&mut self, columns: Vec<Box<dyn ColumnBuilder>>) -> Result<(), RuleError>;
    fn get_rules(&self) -> HashMap<String, Vec<String>>;
}
