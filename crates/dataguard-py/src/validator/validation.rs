use crate::columns;
use crate::rules::core::Rule as PyRule;
use dataguard_core::{
    ColumnRule as CoreRule, ColumnType as CoreType, RuleError, Validator as CoreValidator,
};
use pyo3::{exceptions::PyIOError, prelude::*};
use std::collections::HashMap;

/// Python-facing Validator that wraps the core Rust validator.
/// This is a thin PyO3 bridge that converts Python types to core types.
#[pyclass(name = "Validator")]
pub struct Validator {
    core_validator: CoreValidator,
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl Validator {
    /// Create a new Validator instance.
    #[new]
    pub fn new() -> Self {
        Self {
            core_validator: CoreValidator::new(),
        }
    }

    /// Compiles and commits a list of column configurations to the validator.
    /// This method transforms the Python Column DTOs into core Column types
    /// and delegates to the core validator.
    ///
    /// Args:
    ///     columns (list[Column]): A list of configured Column objects from Python.
    pub fn commit(&mut self, columns: Vec<columns::Column>) -> PyResult<()> {
        // Convert PyO3 columns to core columns
        let core_columns: Vec<CoreColumn> = columns
            .into_iter()
            .map(|py_col| self.convert_column(py_col))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        // Delegate to core validator
        self.core_validator
            .commit(core_columns)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Validate a CSV file against the committed rules.
    ///
    /// Args:
    ///     path (str): Path to the CSV file to validate.
    ///     print_report (bool): Whether to print the validation report.
    ///
    /// Returns:
    ///     int: The number of validation errors found.
    pub fn validate_csv(&mut self, path: &str, print_report: bool) -> PyResult<usize> {
        // Direct delegation to core validator
        self.core_validator
            .validate_csv(path, print_report)
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Get a summary of the configured columns and their rules.
    ///
    /// Returns:
    ///     dict: A dictionary of the configured columns.
    pub fn get_rules(&self) -> PyResult<HashMap<String, Vec<String>>> {
        Ok(self.core_validator.get_rules())
    }
}

impl Validator {
    /// Private helper to convert PyO3 Column DTO → Core Column.
    fn convert_column(&self, py_col: columns::Column) -> Result<CoreColumn, RuleError> {
        // Convert column type string to CoreType enum
        let column_type = match py_col.column_type.as_str() {
            "string" => CoreType::String,
            "integer" => CoreType::Integer,
            "float" => CoreType::Float,
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "Unknown column type: '{}'",
                    py_col.column_type
                )))
            }
        };

        // Convert PyO3 rules to core rules
        let mut rules = Vec::new();
        for py_rule in py_col.rules {
            rules.push(self.convert_rule(py_rule)?);
        }

        // Add unicity rule if present
        if py_col.unicity.is_some() {
            rules.push(CoreRule::Unicity);
        }

        Ok(CoreColumn::new(py_col.name, column_type, rules))
    }

    /// Private helper to convert PyO3 Rule enum → Core ColumnRule enum.
    fn convert_rule(&self, py_rule: PyRule) -> Result<CoreRule, RuleError> {
        match py_rule {
            // String rules
            PyRule::StringLength { min, max } => Ok(CoreRule::StringLength { min, max }),
            PyRule::StringRegex { pattern, flag } => Ok(CoreRule::StringRegex {
                pattern,
                flags: flag,
            }),
            PyRule::StringMembers { members } => Ok(CoreRule::StringMembers { members }),

            // Numeric rules
            PyRule::NumericRange { min, max } => Ok(CoreRule::NumericRange { min, max }),
            PyRule::Monotonicity { asc } => Ok(CoreRule::Monotonicity { ascending: asc }),

            // Generic rules (handled separately in convert_column)
            PyRule::Unicity {} => {
                // This shouldn't be in the rules list, it should be in the unicity field
                // But handle it gracefully
                Ok(CoreRule::Unicity)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::core::Rule as PyRule;

    #[test]
    fn test_convert_string_column() {
        let validator = Validator::new();
        let py_col = columns::Column::new(
            "test".to_string(),
            "string".to_string(),
            vec![
                PyRule::StringLength {
                    min: Some(1),
                    max: Some(10),
                },
                PyRule::StringRegex {
                    pattern: "^test$".to_string(),
                    flag: None,
                },
            ],
            None,
        );

        let core_col = validator.convert_column(py_col).unwrap();
        assert_eq!(core_col.name, "test");
        assert_eq!(core_col.column_type, CoreType::String);
        assert_eq!(core_col.rules.len(), 2);
    }

    #[test]
    fn test_convert_integer_column() {
        let validator = Validator::new();
        let py_col = columns::Column::new(
            "age".to_string(),
            "integer".to_string(),
            vec![PyRule::NumericRange {
                min: Some(0.0),
                max: Some(120.0),
            }],
            None,
        );

        let core_col = validator.convert_column(py_col).unwrap();
        assert_eq!(core_col.name, "age");
        assert_eq!(core_col.column_type, CoreType::Integer);
        assert_eq!(core_col.rules.len(), 1);
    }

    #[test]
    fn test_convert_float_column() {
        let validator = Validator::new();
        let py_col = columns::Column::new(
            "price".to_string(),
            "float".to_string(),
            vec![
                PyRule::NumericRange {
                    min: Some(0.0),
                    max: None,
                },
                PyRule::Monotonicity { asc: true },
            ],
            None,
        );

        let core_col = validator.convert_column(py_col).unwrap();
        assert_eq!(core_col.name, "price");
        assert_eq!(core_col.column_type, CoreType::Float);
        assert_eq!(core_col.rules.len(), 2);
    }

    #[test]
    fn test_convert_column_with_unicity() {
        let validator = Validator::new();
        let py_col = columns::Column::new(
            "id".to_string(),
            "string".to_string(),
            vec![],
            Some(PyRule::Unicity {}),
        );

        let core_col = validator.convert_column(py_col).unwrap();
        assert_eq!(core_col.name, "id");
        assert_eq!(core_col.rules.len(), 1); // Unicity should be added to rules
    }
}
