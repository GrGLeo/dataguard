//! Validator and executable column definitions.
//!
//! This module provides the main entry point (`Validator`) for managing and validating
//! multiple tables, as well as the internal `ExecutableColumn` representation used during
//! validation execution.

use crate::errors::RuleError;
use crate::rules::date::{DateRule, DateTypeCheck};
use crate::rules::generic::{TypeCheck, UnicityCheck};
use crate::rules::numeric::NumericRule;
use crate::rules::relations::RelationRule;
use crate::rules::string::StringRule;
use crate::rules::NullCheck;
use crate::{CsvTable, Table, ValidationResult};
use arrow::datatypes::{Float64Type, Int64Type};
use std::collections::HashMap;

/// Compiled, executable validation rules for a column.
///
/// After user-defined rules (via builders) are compiled, they are stored in this enum
/// which separates columns by their data type. Each variant contains:
/// - Column name
/// - Type-specific validation rules (trait objects)
/// - Type checking logic (for CSV where all data starts as strings)
/// - Optional unicity checking
/// - Optional null checking
pub enum ExecutableColumn {
    /// String column with UTF-8 validation rules.
    String {
        /// Column name (must match schema)
        name: String,
        /// Domain-level string rules (length, regex, membership)
        rules: Vec<Box<dyn StringRule>>,
        /// Type checking (CSV: string → validated string)
        type_check: Option<TypeCheck>,
        /// Optional uniqueness constraint
        unicity_check: Option<UnicityCheck>,
        /// Optional null constraint
        null_check: Option<NullCheck>,
    },
    /// Integer column with i64 validation rules.
    Integer {
        /// Column name (must match schema)
        name: String,
        /// Domain-level numeric rules (range, monotonicity)
        rules: Vec<Box<dyn NumericRule<Int64Type>>>,
        /// Type checking (CSV: string → i64)
        type_check: Option<TypeCheck>,
        /// Optional uniqueness constraint
        unicity_check: Option<UnicityCheck>,
        /// Optional null constraint
        null_check: Option<NullCheck>,
    },
    /// Float column with f64 validation rules.
    Float {
        /// Column name (must match schema)
        name: String,
        /// Domain-level numeric rules (range, monotonicity)
        rules: Vec<Box<dyn NumericRule<Float64Type>>>,
        /// Type checking (CSV: string → f64)
        type_check: Option<TypeCheck>,
        /// Optional uniqueness constraint
        unicity_check: Option<UnicityCheck>,
        /// Optional null constraint
        null_check: Option<NullCheck>,
    },
    /// Date column with DateType32 validation rules.
    Date {
        /// Column name (must match schema)
        name: String,
        /// Domain-level numeric rules ()
        rules: Vec<Box<dyn DateRule>>,
        /// Type checking (CSV: string → f64)
        type_check: Option<DateTypeCheck>,
        /// Optional uniqueness constraint
        unicity_check: Option<UnicityCheck>,
        /// Optional null constraint
        null_check: Option<NullCheck>,
    },
}

impl ExecutableColumn {
    /// Get the column name.
    ///
    /// Returns a clone of the column name string. This is used by the validation
    /// engine to match columns in RecordBatches with their validation rules.
    pub fn get_name(&self) -> String {
        match self {
            ExecutableColumn::String { name, .. } => name.clone(),
            ExecutableColumn::Integer { name, .. } => name.clone(),
            ExecutableColumn::Float { name, .. } => name.clone(),
            ExecutableColumn::Date { name, .. } => name.clone(),
        }
    }

    /// Check if this column has a uniqueness constraint.
    ///
    /// Used by the validation engine to determine which columns need global
    /// uniqueness tracking across batches.
    pub fn has_unicity(&self) -> bool {
        match self {
            ExecutableColumn::String { unicity_check, .. } => unicity_check.is_some(),
            ExecutableColumn::Integer { unicity_check, .. } => unicity_check.is_some(),
            ExecutableColumn::Float { unicity_check, .. } => unicity_check.is_some(),
            ExecutableColumn::Date { unicity_check, .. } => unicity_check.is_some(),
        }
    }
}

pub struct ExecutableRelation {
    pub names: [String; 2],
    pub rules: Vec<Box<dyn RelationRule>>,
}

impl ExecutableRelation {
    pub fn new(names: [String; 2], rules: Vec<Box<dyn RelationRule>>) -> Self {
        Self { names, rules }
    }
}

/// Main entry point for validating tables.
///
/// The `Validator` manages multiple tables (currently CSV, future: Parquet, SQL)
/// and provides methods to validate them individually or in batch.
///
/// # Design
///
/// - Acts as a registry of tables
/// - Each table is independently configured with its own rules
/// - Validation can be performed on individual tables or all tables at once
/// - Thread-safe: tables can be validated in parallel
pub struct Validator {
    tables: HashMap<String, CsvTable>,
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator {
    /// Create a new empty validator.
    ///
    /// Tables must be added via `add_table()` before validation.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Add a table to the validator.
    ///
    /// The table must be configured (via `commit()`) before being added.
    /// If a table with the same name already exists, it will be replaced.
    ///
    /// # Arguments
    ///
    /// * `name` - Identifier for the table (used in `validate_table()`)
    /// * `table` - Configured table instance
    pub fn add_table(&mut self, name: String, table: CsvTable) {
        let _ = self.tables.insert(name, table);
    }

    /// Validate a specific table by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name used when adding the table via `add_table()`
    ///
    /// # Returns
    ///
    /// * `Ok(ValidationResult)` - Validation completed, may contain errors
    /// * `Err(RuleError::TableNotFound)` - No table with this name exists
    /// * `Err(RuleError::...)` - Other validation errors (IO, etc.)
    pub fn validate_table(&mut self, name: String) -> Result<ValidationResult, RuleError> {
        if let Some(table) = self.tables.get_mut(&name) {
            table.validate()
        } else {
            Err(RuleError::TableNotFound(name.to_string()))
        }
    }

    /// Validate all tables in the validator.
    ///
    /// Validates each table sequentially and collects results.
    /// If any table fails to validate, the error is propagated immediately
    /// and remaining tables are not validated.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<ValidationResult>)` - All tables validated
    /// * `Err(RuleError)` - First error encountered
    pub fn validate_all(&mut self) -> Result<Vec<ValidationResult>, RuleError> {
        let mut results = Vec::new();
        for (_name, table) in self.tables.iter_mut() {
            let result = table.validate()?;
            results.push(result);
        }
        Ok(results)
    }
}
