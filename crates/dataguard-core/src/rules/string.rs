use std::collections::HashSet;

use arrow::{
    array::{Int32Array, StringArray},
    compute::{self},
};
use arrow_array::Array;
use arrow_string::length::length;
use xxhash_rust::xxh3::xxh3_64;

use crate::{errors::RuleError, utils::hasher::Xxh3Builder};

/// A trait for defining validation rules on Arrow arrays.
pub trait StringRule: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> String;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &StringArray, column: String) -> Result<usize, RuleError>;
}

/// A rule to check the length of strings in a `StringArray`.
pub struct StringLengthCheck {
    name: String,
    min: Option<usize>,
    max: Option<usize>,
}

impl StringLengthCheck {
    pub fn new(name: String, min: Option<usize>, max: Option<usize>) -> Self {
        Self { name, min, max }
    }
}

impl StringRule for StringLengthCheck {
    fn name(&self) -> String {
        self.name.clone()
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
                for i in len_array.iter().flatten() {
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
                Ok(counter as usize)
            }
            Err(e) => Err(RuleError::ArrowError(e)),
        }
    }
}

/// A rule to check if strings in a `StringArray` match a regex pattern.
pub struct RegexMatch {
    name: String,
    pattern: String,
    flag: Option<String>,
}

impl RegexMatch {
    pub fn new(name: String, pattern: String, flag: Option<String>) -> Self {
        Self {
            name,
            pattern,
            flag,
        }
    }
}

impl StringRule for RegexMatch {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn validate(&self, array: &StringArray, column: String) -> Result<usize, RuleError> {
        let flag = self.flag.as_deref();
        if let Ok(match_array) = compute::regexp_is_match_scalar(array, self.pattern.as_str(), flag)
        {
            let n = match_array.len();
            let null_value = match_array.null_count();
            let true_count = match_array.true_count();
            let violations = n - true_count - null_value;
            Ok(violations)
        } else {
            Err(RuleError::ValidationError(column))
        }
    }
}

pub struct IsInCheck {
    name: String,
    members: HashSet<u64, Xxh3Builder>,
}

impl IsInCheck {
    pub fn new(name: String, members: Vec<String>) -> Self {
        let mut hashset = HashSet::with_hasher(Xxh3Builder);
        members.into_iter().for_each(|m| {
            let hash = xxh3_64(m.as_bytes());
            let _ = hashset.insert(hash);
        });
        Self {
            name,
            members: hashset,
        }
    }
}

impl StringRule for IsInCheck {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn validate(&self, array: &StringArray, _column: String) -> Result<usize, RuleError> {
        let errors = array
            .iter()
            .map(|v| {
                if let Some(s) = v {
                    let s_hash = xxh3_64(s.as_bytes());
                    if !self.members.contains(&s_hash) {
                        1
                    } else {
                        0
                    }
                } else {
                    0
                }
            })
            .sum();
        Ok(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_string_length_check() {
        let rule = StringLengthCheck::new("string_length_test".to_string(), Some(3), Some(5));
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
        let rule = StringLengthCheck::new("string_length_test".to_string(), Some(3), None);
        let array = StringArray::from(vec!["a", "ab", "abc", "abcd"]);
        // "a" (len 1, <3) -> 1 error
        // "ab" (len 2, <3) -> 1 error
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_string_length_check_max_only() {
        let rule = StringLengthCheck::new("string_length_test".to_string(), None, Some(3));
        let array = StringArray::from(vec!["a", "ab", "abc", "abcd", "abcde"]);
        // "abcd" (len 4, >3) -> 1 error
        // "abcde" (len 5, >3) -> 1 error
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_regex_match() {
        let rule = RegexMatch::new("regex_match_test".to_string(), r"^\d{3}$".to_string(), None); // Expects exactly 3 digits
        let array = StringArray::from(vec![
            Some("123"),  // ok
            Some("abc"),  // error
            Some("12"),   // error
            Some("1234"), // error
            None,         // ok
            Some("456"),  // ok
        ]);
        // "abc", "12", "1234" are errors (3)
        // None is not counted as an error
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_regex_match_with_flags() {
        // Case-insensitive match
        let rule = RegexMatch::new(
            "regex_match_test".to_string(),
            "abc".to_string(),
            Some("i".to_string()),
        );
        let array = StringArray::from(vec![
            Some("ABC"), // ok
            Some("def"), // error
            Some("aBc"), // ok
        ]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_is_in_check_basic() {
        let members = vec!["apple".to_string(), "banana".to_string()];
        let rule = IsInCheck::new("is_in_test".to_string(), members);
        let array = StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("orange"), // Not in members
            None,           // Null value
            Some(""),       // Empty string
        ]);
        // Expected errors: "orange", "" (2 errors)
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_is_in_check_case_sensitivity() {
        let members = vec!["apple".to_string()];
        let rule = IsInCheck::new("is_in_test".to_string(), members);
        let array = StringArray::from(vec![
            Some("apple"),
            Some("Apple"), // Different case, not in members
        ]);
        // Expected errors: "Apple" (1 error)
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_is_in_check_empty_members() {
        let members: Vec<String> = vec![];
        let rule = IsInCheck::new("is_in_test".to_string(), members);
        let array = StringArray::from(vec![
            Some("apple"),  // Not in empty members
            Some("banana"), // Not in empty members
            None,           // Null value
        ]);
        // Expected errors: "apple", "banana" (2 errors)
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }
}
