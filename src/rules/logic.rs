use arrow::{
    array::{Array, Int32Array, Int64Array, StringArray},
    compute,
    datatypes::DataType,
};
use arrow_string::length::length;

use crate::errors::RuleError;

/// A trait for defining validation rules on Arrow arrays.
pub trait StringRule: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> &'static str;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &StringArray, column: String) -> Result<usize, RuleError>;
}

pub trait IntegerRule: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> &'static str;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &Int64Array) -> Result<usize, RuleError>;
}

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

    pub fn validate(&self, array: &dyn Array) -> Result<usize, &'static str> {
        if let Ok(casted_array) = compute::cast(array, &self.expected) {
            Ok(casted_array.null_count())
        } else {
            Err("Failed to cast to expected Type")
        }
    }
}

/// A rule to check the length of strings in a `StringArray`.
pub struct StringLengthCheck {
    min: Option<usize>,
    max: Option<usize>,
}

impl StringLengthCheck {
    pub fn new(min: Option<usize>, max: Option<usize>) -> Self {
        Self { min, max }
    }
}

impl StringRule for StringLengthCheck {
    fn name(&self) -> &'static str {
        "StringLengthCheck"
    }

    fn validate(&self, array: &StringArray, column: String) -> Result<usize, RuleError> {
        let res = length(array);
        match res {
            Ok(dyn_array) => {
                let len_array =
                    dyn_array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| {
                            RuleError::ValidationError(format!(
                                "StringLengthCheck applied to a non-string column '{}'",
                                column
                            ))
                        })?;
                let mut counter: u32 = 0;
                for value in len_array.iter() {
                    match value {
                        Some(i) => {
                            if let Some(min) = self.min {
                                if i < min as i32 {
                                    counter += 1
                                }
                            }
                            if let Some(max) = self.max {
                                if i > max as i32 {
                                    counter += 1
                                }
                            }
                        }
                        None => counter += 1,
                    }
                }
                Ok(counter as usize)
            }
            Err(e) => return Err(RuleError::ArrowError(e)),
        }
    }
}

/// A rule to check if strings in a `StringArray` match a regex pattern.
pub struct RegexMatch {
    pattern: String,
    flag: Option<String>,
}

impl RegexMatch {
    pub fn new(pattern: String, flag: Option<String>) -> Self {
        Self { pattern, flag }
    }
}

impl StringRule for RegexMatch {
    fn name(&self) -> &'static str {
        "RegexMatch"
    }

    fn validate(&self, array: &StringArray, column: String) -> Result<usize, RuleError> {
        let flag = self.flag.as_deref();
        if let Ok(match_array) = compute::regexp_is_match_scalar(array, self.pattern.as_str(), flag)
        {
            let n = match_array.len();
            let true_count = match_array.true_count();
            let violations = n - true_count;
            Ok(violations)
        } else {
            Err(RuleError::ValidationError(column))
        }
    }
}

#[cfg(test)]
mod tests {}
