use thiserror::Error;

#[derive(Error, Debug)]
pub enum RuleError {
    /// Column not found in the current RecordBatch
    #[error("Column '{0}' not found in RecordBatch")]
    ColumnNotFound(String),

    /// The value could not be cast to the expected type
    #[error("Failed to cast column '{0}' to type {1}")]
    TypeCastError(String, String),

    /// The Arrow kernel produced an error (e.g., unsupported cast)
    #[error("Arrow computation error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// CSV reading or IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Generic validation error with message
    #[error("Validation error: {0}")]
    ValidationError(String),
}

