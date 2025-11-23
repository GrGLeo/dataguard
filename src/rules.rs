use arrow::{
    array::{Array, ArrayRef, Int32Array, StringArray},
    compute,
    datatypes::DataType,
};
use arrow_string::length::length;
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
    pub fn new(column: String, pattern: String, flag: Option<String>) -> Self {
        Self {
            column,
            pattern,
            flag,
        }
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

pub enum Case {
    Lower,
    Upper,
}

pub struct CaseCheck {
    column: String,
    case: Case,
}

impl CaseCheck {
    pub fn new(column: String, case: &str) -> Result<Self, RuleError> {
        let c = match case {
            "lower" => Case::Lower,
            "upper" => Case::Upper,
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "case: '{}' not recognized",
                    case
                )));
            }
        };

        Ok(Self { column, case: c })
    }
}

impl Rule for CaseCheck {
    fn name(&self) -> &str {
        "CaseCheck"
    }

    fn validate(&self, array: &ArrayRef) -> Result<usize, RuleError> {
        arrow_string::length::length(array);
        todo!()
    }
}

pub enum Operator {
    Gt,
    Lt,
}

pub struct StringLengthCheck {
    column: String,
    length: usize,
    operator: Operator,
}

impl StringLengthCheck {
    pub fn new(column: String, length: usize, op: Operator) -> Self {
        Self {
            column,
            length,
            operator: op,
        }
    }
}

impl Rule for StringLengthCheck {
    fn name(&self) -> &str {
        "StringLengthCheck"
    }

    fn validate(&self, array: &ArrayRef) -> Result<usize, RuleError> {
        if array.data_type() != &DataType::Utf8 {
            return Err(RuleError::ValidationError(format!(
                "StringLengthCheck applied to a non-string column '{}'",
                self.column
            )));
        }
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
                                self.column
                            ))
                        })?;
                let mut counter: u32 = 0;
                for value in len_array.iter() {
                    match value {
                        Some(i) => {
                            match self.operator {
                                Operator::Lt => {
                                    if i < self.length as i32 {
                                        counter += 1;
                                    }
                                }
                                Operator::Le => {
                                    if i <= self.length as i32 {
                                        counter += 1;
                                    }
                                }
                                Operator::Gt => {
                                    if i > self.length as i32 {
                                        counter += 1;
                                    }
                                }
                                Operator::Ge => {
                                    if i >= self.length as i32 {
                                        counter += 1;
                                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_type_check_string_success() {
        let rule = TypeCheck::new("col".to_string(), DataType::Utf8);
        let array = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // No nulls, cast succeeds
    }

    #[test]
    fn test_type_check_string_with_nulls() {
        let rule = TypeCheck::new("col".to_string(), DataType::Utf8);
        let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // One null
    }

    #[test]
    fn test_type_check_string_to_int_with_violations() {
        let rule = TypeCheck::new("col".to_string(), DataType::Int64);
        let array = Arc::new(StringArray::from(vec!["abc", "def", "ghi"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3); // All invalid, become null
    }

    #[test]
    fn test_regex_match_valid_creation() {
        let rule = RegexMatch::new("col".to_string(), r"^\d+$", None);
        assert!(rule.is_ok());
    }

    #[test]
    fn test_regex_match_invalid_regex() {
        let rule = RegexMatch::new("col".to_string(), r"[", None);
        assert!(rule.is_err());
    }

    #[test]
    fn test_regex_match_validation_success() {
        let rule = RegexMatch::new("col".to_string(), r"^\d+$", None).unwrap();
        let array = Arc::new(StringArray::from(vec!["123", "456", "789"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // All match
    }

    #[test]
    fn test_regex_match_validation_violations() {
        let rule = RegexMatch::new("col".to_string(), r"^\d+$", None).unwrap();
        let array = Arc::new(StringArray::from(vec!["123", "abc", "789"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // One violation
    }

    #[test]
    fn test_regex_match_non_string_array() {
        let rule = RegexMatch::new("col".to_string(), r"^\d+$", None).unwrap();
        let array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_err());
    }

    #[test]
    fn test_string_length_lt_success() {
        let rule = StringLengthCheck::new("col".to_string(), 3, "lt").unwrap();
        let array = Arc::new(StringArray::from(vec!["abc", "def", "ghi"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // None < 3
    }

    #[test]
    fn test_string_length_lt_violations() {
        let rule = StringLengthCheck::new("col".to_string(), 3, "lt").unwrap();
        let array = Arc::new(StringArray::from(vec!["ab", "def", "g"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2); // "ab" and "g" < 3
    }

    #[test]
    fn test_string_length_lt_with_nulls() {
        let rule = StringLengthCheck::new("col".to_string(), 3, "lt").unwrap();
        let array = Arc::new(StringArray::from(vec![Some("ab"), None, Some("def")])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2); // "ab" < 3 and null
    }

    #[test]
    fn test_string_length_lt_non_string() {
        let rule = StringLengthCheck::new("col".to_string(), 3, "lt").unwrap();
        let array = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_err());
    }

    #[test]
    fn test_string_length_ge_success() {
        let rule = StringLengthCheck::new("col".to_string(), 3, "ge").unwrap();
        let array = Arc::new(StringArray::from(vec!["abc", "def", "ghi"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3); // All >= 3
    }

    #[test]
    fn test_string_length_ge_violations() {
        let rule = StringLengthCheck::new("col".to_string(), 3, "ge").unwrap();
        let array = Arc::new(StringArray::from(vec!["ab", "def", "g"])) as ArrayRef;
        let result = rule.validate(&array);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // Only "def" >= 3
    }

    // CaseCheck tests - placeholder since implementation is incomplete
    #[test]
    #[should_panic(expected = "not yet implemented")]
    fn test_case_check_lower() {
        let rule = CaseCheck::new("col".to_string(), "lower").unwrap();
        let array = Arc::new(StringArray::from(vec!["abc", "def"])) as ArrayRef;
        let _ = rule.validate(&array); // Will panic due to todo!()
    }
}
