use arrow::{
    array::{Int32Array, StringArray},
    compute::{self},
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
                            if let Some(min) = self.min
                                && i < min as i32
                            {
                                counter += 1
                            }
                            if let Some(max) = self.max
                                && i > max as i32
                            {
                                counter += 1
                            }
                        }
                        None => counter += 1,
                    }
                }
                Ok(counter as usize)
            }
            Err(e) => Err(RuleError::ArrowError(e)),
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
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_string_length_check() {
        let rule = StringLengthCheck::new(Some(3), Some(5));
        // "a" (error), "abc", "abcde", "abcdef" (error), "" (error), NULL (error according to current code)
        let array = StringArray::from(vec![
            Some("a"),
            Some("abc"),
            Some("abcde"),
            Some("abcdef"),
            Some(""),
        ]);
        // "a" (len 1, <3) -> 1 error
        // "abcdef" (len 6, >5) -> 1 error
        // "" (len 0, <3) -> 1 error
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_string_length_check_min_only() {
        let rule = StringLengthCheck::new(Some(3), None);
        let array = StringArray::from(vec!["a", "ab", "abc", "abcd"]);
        // "a" (len 1, <3) -> 1 error
        // "ab" (len 2, <3) -> 1 error
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_string_length_check_max_only() {
        let rule = StringLengthCheck::new(None, Some(3));
        let array = StringArray::from(vec!["a", "ab", "abc", "abcd", "abcde"]);
        // "abcd" (len 4, >3) -> 1 error
        // "abcde" (len 5, >3) -> 1 error
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_regex_match() {
        let rule = RegexMatch::new(r"^\d{3}$".to_string(), None); // Expects exactly 3 digits
        let array = StringArray::from(vec![
            Some("123"),  // ok
            Some("abc"),  // error
            Some("12"),   // error
            Some("1234"), // error
            None,         // error (non-match for null, as per n - true_count logic)
            Some("456"),  // ok
        ]);
        // "abc", "12", "1234" are errors (3)
        // None is also counted as an error (1)
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 4);
    }

    #[test]
    fn test_regex_match_with_flags() {
        // Case-insensitive match
        let rule = RegexMatch::new("abc".to_string(), Some("i".to_string()));
        let array = StringArray::from(vec![
            Some("ABC"), // ok
            Some("def"), // error
            Some("aBc"), // ok
        ]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 1);
    }
}
