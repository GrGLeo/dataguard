use arrow::{
    array::{Array, Int32Array, StringArray},
    compute::{self},
    datatypes::DataType,
};
use arrow_array::{ArrowNumericType, PrimitiveArray};
use arrow_ord::cmp::{gt, lt};
use arrow_string::length::length;
use num_traits::Num;
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::errors::RuleError;

/// A trait for defining validation rules on Arrow arrays.
pub trait StringRule: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> &'static str;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &StringArray, column: String) -> Result<usize, RuleError>;
}

pub trait NumericRule<T: ArrowNumericType>: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> &'static str;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &PrimitiveArray<T>, column: String) -> Result<usize, RuleError>;
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

pub struct Range<N: Num + PartialOrd + Copy + Debug> {
    min: Option<N>,
    max: Option<N>,
}

impl<N> Range<N>
where
    N: Num + PartialOrd + Copy + Debug,
{
    pub fn new(min: Option<N>, max: Option<N>) -> Self {
        Self { min, max }
    }
}

impl<T, N> NumericRule<T> for Range<N>
where
    T: ArrowNumericType<Native = N>,
    N: Num + PartialOrd + Copy + Debug + Send + Sync,
{
    fn name(&self) -> &'static str {
        "NumericRange"
    }

    fn validate(&self, array: &PrimitiveArray<T>, _column: String) -> Result<usize, RuleError> {
        let mut counter: usize = 0;
        for value in array.iter() {
            match value {
                Some(i) => {
                    if let Some(min) = self.min
                        && i < min
                    {
                        counter += 1
                    }
                    if let Some(max) = self.max
                        && i > max
                    {
                        counter += 1
                    }
                }
                None => counter += 1,
            }
        }
        Ok(counter)
    }
}

pub struct Monotonicity<N> {
    asc: bool,
    _phantom: PhantomData<N>, // To tie N to the struct
}

impl<N: PartialOrd> Monotonicity<N> {
    pub fn new(asc: bool) -> Self {
        Self {
            asc,
            _phantom: PhantomData,
        }
    }
}

impl<N: PartialOrd> Default for Monotonicity<N> {
    fn default() -> Self {
        Self {
            asc: true,
            _phantom: PhantomData,
        }
    }
}

impl<T, N> NumericRule<T> for Monotonicity<N>
where
    T: ArrowNumericType<Native = N>,
    N: PartialOrd + Copy + Debug + Send + Sync,
{
    fn name(&self) -> &'static str {
        "Monotonicity"
    }

    fn validate(&self, array: &PrimitiveArray<T>, _column: String) -> Result<usize, RuleError> {
        if array.len() <= 1 {
            return Ok(0);
        };
        let predecessor = array.slice(0, array.len() - 1);
        let successor = array.slice(1, array.len() - 1);

        let predecessor_array = predecessor
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();
        let successor_array = successor
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();

        let comparaison = match self.asc {
            true => lt(successor_array, predecessor_array),
            false => gt(successor_array, predecessor_array),
        };
        let violation = comparaison.map_err(RuleError::ArrowError)?.true_count();
        Ok(violation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

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

    #[test]
    fn test_min_range_integer_with_null() {
        let rule = Range::new(Some(5i64), None);
        let array = Int64Array::from(vec![Some(1), Some(6), Some(3), Some(2), None]);
        // We expect 4 errors here index 0, 2, 3, 4
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 4);
    }

    #[test]
    fn test_min_range_integer() {
        let rule = Range::new(Some(5i64), None);
        let array = Int64Array::from(vec![Some(7), Some(6), Some(5), Some(2), Some(4)]);
        // We expect 2 errors here index 3, 4
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_max_range_integer_with_null() {
        let rule = Range::new(None, Some(5i64));
        let array = Int64Array::from(vec![Some(1), Some(6), Some(3), Some(2), None]);
        // We expect 2 errors here index 1, 4
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_max_range_integer() {
        let rule = Range::new(None, Some(5i64));
        let array = Int64Array::from(vec![Some(7), Some(6), Some(5), Some(2), Some(4)]);
        // We expect 2 errors here index 0, 1
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_range_between_integer_with_null() {
        let rule = Range::new(Some(2i64), Some(4i64));
        let array = Int64Array::from(vec![Some(1), Some(4), Some(6), Some(3), Some(2), None]);
        // We expect 3 errors here: 0, 2, 5
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_range_between_integer() {
        let rule = Range::new(Some(2i64), Some(4i64));
        let array = Int64Array::from(vec![Some(7), Some(6), Some(5), Some(2), Some(4)]);
        // We expect 2 errors here index 0, 1, 2
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_monotonicity_asc_valid() {
        let rule = Monotonicity::<i64>::default();
        let array = Int64Array::from(vec![1, 5, 5, 10]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_monotonicity_asc_violation() {
        let rule = Monotonicity::<i64>::default();
        let array = Int64Array::from(vec![1, 5, 4, 3, 10]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_monotonicity_desc_valid() {
        let rule = Monotonicity::<i64>::new(false);
        //
        //
        let array = Int64Array::from(vec![10, 5, 5, 1]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_monotonicity_desc_violation() {
        let rule = Monotonicity::<i64>::new(false);
        let array = Int64Array::from(vec![10, 3, 4, 5, 1]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_is_positive() {
        let rule = Range::new(Some(1i64), None);
        let array = Int64Array::from(vec![Some(1), Some(0), Some(5), Some(-2), None]);
        // 0, -2, None should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_is_negative() {
        let rule = Range::new(None, Some(-1i64));
        let array = Int64Array::from(vec![Some(-1), Some(0), Some(-5), Some(2), None]);
        // 0, 2, None should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_is_non_positive() {
        let rule = Range::new(None, Some(0i64));
        let array = Int64Array::from(vec![Some(-1), Some(0), Some(5), Some(-2), None]);
        // 5, None should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_is_non_negative() {
        let rule = Range::new(Some(0i64), None);
        let array = Int64Array::from(vec![Some(1), Some(0), Some(5), Some(-2), None]);
        // -2, None should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }
}
