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
    #[error("Rule '{rule_name}' for column '{column_name}' requires field '{field_name}'")]
    MissingRuleField {
        rule_name: String,
        column_name: String,
        field_name: String,
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
    #[error("Validation error: {0}")]
    ValidationError(#[from] RuleError),
}
