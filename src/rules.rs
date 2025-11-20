use arrow::{array::RecordBatch, datatypes::DataType};

use crate::errors::RuleError;

pub trait Rule: Send + Sync {
    fn validate(&self, batch: &RecordBatch) -> Result<usize, RuleError>;
    fn name(&self) -> &str;
}

pub struct TypeCheck {
    column: String,
    expected: DataType,
}

impl TypeCheck {
    pub fn new(column: String, expected: DataType) -> Self {
        Self {
            column,
            expected,
        }
    }
}


impl Rule for TypeCheck {
    fn name(&self) -> &str {
        "TypeCheck"
    }

    fn validate(&self, batch: &RecordBatch) -> Result<usize, RuleError> {
        todo!()
    }
}

pub struct NotUnique {
    column: String,
}

impl NotUnique {
    pub fn new(column: String) -> Self {
        Self {
            column
        }
    }
}

impl Rule for NotUnique {
    fn name(&self) -> &str {
        "NotUnique"
    }

    fn validate(&self, batch: &RecordBatch) -> Result<usize, RuleError> {
        todo!()
    }
}
