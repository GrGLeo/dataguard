use crate::errors::RuleError;
use crate::{CsvTable, Table, ValidationResult};
use std::collections::HashMap;

/// Core validator (no PyO3 dependencies)
pub struct Validator {
    tables: HashMap<String, CsvTable>,
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator {
    /// Create a new Validator instance
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, name: String, table: CsvTable) {
        let _ = self.tables.insert(name, table);
    }

    pub fn validate_table(&mut self, name: String) -> Result<ValidationResult, RuleError> {
        if let Some(table) = self.tables.get_mut(&name) {
            table.validate()
        } else {
            Err(RuleError::TableNotFound(name.to_string()))
        }
    }

    pub fn validate_all(&mut self) -> Result<Vec<ValidationResult>, RuleError> {
        let mut results = Vec::new();
        for (_name, table) in self.tables.iter_mut() {
            let result = table.validate()?;
            results.push(result);
        }
        Ok(results)
    }
}
