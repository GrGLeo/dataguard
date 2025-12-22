use dataguard_core::RuleError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    // Rule application errors - these need structured handling
    #[error("Unknown rule '{rule_name}' for {column_type} column '{column_name}'")]
    UnknownRule {
        rule_name: String,
        column_type: String,
        column_name: String,
    },
    #[error("Rule '{rule_name}' for column '{column_name}' expected field to be '{field_type}'")]
    WrongRuleData {
        rule_name: String,
        column_name: String,
        field_type: String,
    },
    #[error("Unknown datatype '{datatype}' for column '{column_name}'. Supported: string, integer, float")]
    UnknownDatatype {
        datatype: String,
        column_name: String,
    },
    // Validation errors from core
    #[error("Validation error")]
    ValidationError(#[from] RuleError),
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Error: csv file not found: '{table_path}'")]
    FileNotFound { table_path: String },
    #[error("Column format error: '{column_name}' with type '{type_name}' - {message}")]
    ColumnError {
        column_name: String,
        type_name: String,
        message: String,
    },
    #[error(
        "Table relation error: column '{missing_column}' need to be provided in 'table.column'"
    )]
    RelationError { missing_column: String },
    #[error("Rule logic error: rule '{rule_name}' for '{column_name}' - {message}")]
    RuleError {
        rule_name: String,
        column_name: String,
        message: String,
    },
    #[error("Watch mode can only be use on one table. Found {n_table} in config")]
    TooMuchTable { n_table: usize },
}
