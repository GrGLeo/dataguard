use std::collections::HashMap;

use crate::columns::ColumnBuilder;
use crate::errors::RuleError;
use crate::reader::read_csv_parallel;
use crate::tables::Table;
use crate::validator::ExecutableColumn;
use crate::{compiler, engine, ValidationResult};

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
    fn prepare(&mut self, columns: Vec<Box<dyn ColumnBuilder>>) -> Result<(), RuleError> {
        self.executable_columns = columns
            .into_iter()
            .map(|col| compiler::compile_column(col, true))
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
        let engine = engine::ValidationEngine::new(&self.executable_columns);
        engine.validate_batches(self.table_name.clone(), &batches)
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
                ExecutableColumn::Date {
                    name,
                    rules,
                    type_check,
                    unicity_check,
                    null_check,
                } => {}
            }
        }
        result
    }
}
