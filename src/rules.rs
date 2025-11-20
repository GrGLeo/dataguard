use arrow::{array::ArrayRef, compute, datatypes::DataType};

use crate::errors::RuleError;

pub trait Rule: Send + Sync {
    fn validate(&self, array: &ArrayRef) -> Result<usize, RuleError>;
    fn name(&self) -> &str;
}

pub struct TypeCheck {
    column: String,
    expected: DataType,
}

impl TypeCheck {
    pub fn new(column: String, expected: DataType) -> Self {
        Self { column, expected }
    }
}

impl Rule for TypeCheck {
    fn name(&self) -> &str {
        "TypeCheck"
    }

    fn validate(&self, array: &ArrayRef) -> Result<usize, RuleError> {
        if let Ok(casted_array) = compute::cast(array, &self.expected) {
            Ok(casted_array.null_count())
        } else {
            Err(RuleError::TypeCastError(self.column.clone(), self.expected.to_string()))
        }
    }
}

pub struct NotUnique {
    column: String,
}

impl NotUnique {
    pub fn new(column: String) -> Self {
        Self { column }
    }
}

impl Rule for NotUnique {
    fn name(&self) -> &str {
        "NotUnique"
    }

    fn validate(&self, _array: &ArrayRef) -> Result<usize, RuleError> {
        todo!()
    }
}
