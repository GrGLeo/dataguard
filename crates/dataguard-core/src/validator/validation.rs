use crate::errors::RuleError;
use crate::{CsvTable, Table};
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

    pub fn validate_table(&mut self, name: String) -> Result<(), RuleError> {
        if let Some(table) = self.tables.get_mut(&name) {
            table.validate()
        } else {
            Err(RuleError::TableNotFound(name))
        }
    }

    pub fn validate_all(&mut self) -> Result<(), RuleError> {
        for (name, table) in self.tables.iter_mut() {
            println!("Validation on: {}", name.clone());
            if let Err(e) = table.validate() {
                println!("Validation failed: {}", e)
            };
        }
        Ok(())
    }
}
