use arrow::{
    array::{Array, ArrayRef, StringArray},
    compute,
    datatypes::DataType,
};
use regex::Regex;

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
            Err(RuleError::TypeCastError(
                self.column.clone(),
                self.expected.to_string(),
            ))
        }
    }
}

pub struct RegexMatch {
    column: String,
    pattern: String,
    flag: Option<String>,
}

impl RegexMatch {
    pub fn new(column: String, pattern: &str, flag: Option<&str>) -> Result<Self, RuleError> {
        // We validate the Regex at creation
        let _ = Regex::new(pattern).map_err(|e| {
            RuleError::ValidationError(format!("Invalid regex: '{}' : '{}'", pattern, e))
        })?;

        let flag = flag.map(|f| f.to_string());
        Ok(Self {
            column,
            pattern: pattern.to_string(),
            flag,
        })
    }
}

impl Rule for RegexMatch {
    fn name(&self) -> &str {
        "RegexMatch"
    }

    fn validate(&self, array: &ArrayRef) -> Result<usize, RuleError> {
        if array.data_type() != &DataType::Utf8 {
            return Err(RuleError::ValidationError(format!(
                "RegexMatch applied to a non-string column '{}'",
                self.column
            )));
        }

        let str_arr = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                RuleError::ValidationError(format!(
                    "RegexMatch applied to a non-string column '{}'",
                    self.column
                ))
            })?;

        let flag = self.flag.as_deref();
        if let Ok(match_array) =
            compute::regexp_is_match_scalar(str_arr, self.pattern.as_str(), flag)
        {
            let n = match_array.len();
            let true_count = match_array.true_count();
            let violations = n - true_count;
            Ok(violations)
        } else {
            Err(RuleError::ValidationError(self.column.clone()))
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
