use arrow::{
    array::Array,
    compute::{self},
    datatypes::DataType,
};
use std::sync::Arc;

use crate::errors::RuleError;

pub struct TypeCheck {
    column: String,
    expected: DataType,
}

impl TypeCheck {
    pub fn new(column: String, expected: DataType) -> Self {
        Self { column, expected }
    }

    pub fn name(&self) -> &'static str {
        "TypeCheck"
    }

    pub fn validate(&self, array: &dyn Array) -> Result<(usize, Arc<dyn Array>), RuleError> {
        match compute::cast(array, &self.expected) {
            Ok(casted_array) => {
                let errors = casted_array.null_count() - array.null_count();
                Ok((errors, casted_array))
            }
            Err(e) => Err(RuleError::TypeCastError(self.column.clone(), e.to_string())),
        }
    }
}
