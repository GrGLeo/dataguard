use arrow_array::Date32Array;
use chrono::NaiveDate;

use crate::RuleError;

/// A trait for defining validation rules on Arrow arrays.
pub trait DateRule: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> &'static str;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &Date32Array, column: String) -> Result<usize, RuleError>;
}

pub struct DateBoundaryCheck {
    days: i32,
    after: bool,
}

impl DateBoundaryCheck {
    pub fn new(
        after: bool,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
    ) -> Result<Self, RuleError> {
        let m = month.unwrap_or_else(|| 1) as u32;
        let d = day.unwrap_or_else(|| 1) as u32;
        let res = NaiveDate::from_ymd_opt(year as i32, m, d);
        match res {
            Some(date) => {
                // Here we can unwrap date is correct
                let unix = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = date.signed_duration_since(unix).num_days() as i32;
                return Ok(Self { after, days });
            }
            None => {
                return Err(RuleError::IncorrectDate(year, m, d));
            }
        }
    }
}

impl DateRule for DateBoundaryCheck {
    fn name(&self) -> &'static str {
        "DateBoundaryCheck"
    }

    fn validate(&self, array: &Date32Array, _column: String) -> Result<usize, RuleError> {
        let mut counter = 0;
        for value in array.iter() {
            if let Some(day) = value {
                if self.after {
                    if day <= self.days {
                        counter += 1;
                    }
                } else {
                    if day >= self.days {
                        counter += 1;
                    }
                }
            }
        }
        Ok(counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Date32Array;
    use chrono::NaiveDate;

    /// Helper function to convert a date to days since Unix epoch (1970-01-01)
    fn date_to_days(year: i32, month: u32, day: u32) -> i32 {
        let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
        let unix = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        date.signed_duration_since(unix).num_days() as i32
    }

    // ============================================================================
    // Constructor Tests
    // ============================================================================

    #[test]
    fn test_constructor_valid_date_all_parameters() {
        let result = DateBoundaryCheck::new(true, 2020, Some(6), Some(15));
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, date_to_days(2020, 6, 15));
        assert_eq!(rule.after, true);
    }

    #[test]
    fn test_constructor_valid_date_year_and_month() {
        let result = DateBoundaryCheck::new(false, 2020, Some(6), None);
        assert!(result.is_ok());
        let rule = result.unwrap();
        // Should default to day 1
        assert_eq!(rule.days, date_to_days(2020, 6, 1));
        assert_eq!(rule.after, false);
    }

    #[test]
    fn test_constructor_valid_date_year_only() {
        let result = DateBoundaryCheck::new(true, 2020, None, None);
        assert!(result.is_ok());
        let rule = result.unwrap();
        // Should default to January 1st
        assert_eq!(rule.days, date_to_days(2020, 1, 1));
    }

    #[test]
    fn test_constructor_invalid_month() {
        let result = DateBoundaryCheck::new(true, 2020, Some(13), Some(1));
        assert!(result.is_err());
        match result {
            Err(RuleError::IncorrectDate(year, month, day)) => {
                assert_eq!(year, 2020);
                assert_eq!(month, 13);
                assert_eq!(day, 1);
            }
            _ => panic!("Expected IncorrectDate error"),
        }
    }

    #[test]
    fn test_constructor_invalid_day() {
        let result = DateBoundaryCheck::new(true, 2020, Some(2), Some(30));
        assert!(result.is_err());
        match result {
            Err(RuleError::IncorrectDate(year, month, day)) => {
                assert_eq!(year, 2020);
                assert_eq!(month, 2);
                assert_eq!(day, 30);
            }
            _ => panic!("Expected IncorrectDate error"),
        }
    }

    #[test]
    fn test_constructor_leap_year_feb_29() {
        // 2020 is a leap year, Feb 29 should be valid
        let result = DateBoundaryCheck::new(true, 2020, Some(2), Some(29));
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, date_to_days(2020, 2, 29));
    }

    #[test]
    fn test_constructor_non_leap_year_feb_29() {
        // 2021 is not a leap year, Feb 29 should be invalid
        let result = DateBoundaryCheck::new(true, 2021, Some(2), Some(29));
        assert!(result.is_err());
    }

    #[test]
    fn test_constructor_unix_epoch() {
        let result = DateBoundaryCheck::new(true, 1970, Some(1), Some(1));
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, 0); // Unix epoch is day 0
    }

    #[test]
    fn test_constructor_date_before_epoch() {
        let result = DateBoundaryCheck::new(true, 1969, Some(12), Some(31));
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, -1); // Day before epoch
    }

    #[test]
    fn test_constructor_far_future_date() {
        let result = DateBoundaryCheck::new(true, 2100, Some(1), Some(1));
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, date_to_days(2100, 1, 1));
    }

    #[test]
    fn test_constructor_month_zero_invalid() {
        let result = DateBoundaryCheck::new(true, 2020, Some(0), Some(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_constructor_day_zero_invalid() {
        let result = DateBoundaryCheck::new(true, 2020, Some(1), Some(0));
        assert!(result.is_err());
    }

    // ============================================================================
    // Validation Tests - After (dates should be AFTER threshold)
    // ============================================================================

    #[test]
    fn test_validate_after_all_dates_after_threshold() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();

        // All dates after 2020-01-01
        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 1, 2)),  // After
            Some(date_to_days(2020, 6, 15)), // After
            Some(date_to_days(2021, 1, 1)),  // After
        ]);

        // No violations - all dates are after threshold
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_after_all_dates_before_threshold() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();

        // All dates before 2020-01-01
        let array = Date32Array::from(vec![
            Some(date_to_days(2019, 12, 31)), // Before
            Some(date_to_days(2019, 6, 15)),  // Before
            Some(date_to_days(2018, 1, 1)),   // Before
        ]);

        // All violations - all dates are before threshold
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_validate_after_mixed_dates() {
        // Threshold: 2020-06-01
        let rule = DateBoundaryCheck::new(true, 2020, Some(6), Some(1)).unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 5, 31)),  // Before (violation)
            Some(date_to_days(2020, 6, 2)),   // After (valid)
            Some(date_to_days(2020, 7, 1)),   // After (valid)
            Some(date_to_days(2019, 1, 1)),   // Before (violation)
            Some(date_to_days(2021, 12, 31)), // After (valid)
        ]);

        // 2 violations: 2020-05-31 and 2019-01-01
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_validate_after_date_equal_to_threshold() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();

        // Date exactly equal to threshold
        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 1, 1)), // Equal (should be violation with <=)
        ]);

        // Date equal to threshold counts as violation (not strictly after)
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_validate_after_with_nulls() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(2019, 12, 31)), // Before (violation)
            None,                             // Null (ignored)
            Some(date_to_days(2020, 1, 2)),   // After (valid)
            None,                             // Null (ignored)
            Some(date_to_days(2019, 6, 15)),  // Before (violation)
        ]);

        // 2 violations, nulls are ignored
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    // ============================================================================
    // Validation Tests - Before (dates should be BEFORE threshold)
    // ============================================================================

    #[test]
    fn test_validate_before_all_dates_before_threshold() {
        // Threshold: 2020-12-31
        let rule = DateBoundaryCheck::new(false, 2020, Some(12), Some(31)).unwrap();

        // All dates before 2020-12-31
        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 12, 30)), // Before
            Some(date_to_days(2020, 6, 15)),  // Before
            Some(date_to_days(2019, 1, 1)),   // Before
        ]);

        // No violations - all dates are before threshold
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_before_all_dates_after_threshold() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(false, 2020, Some(1), Some(1)).unwrap();

        // All dates after 2020-01-01
        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 1, 2)),  // After
            Some(date_to_days(2020, 6, 15)), // After
            Some(date_to_days(2021, 1, 1)),  // After
        ]);

        // All violations - all dates are after threshold
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_validate_before_mixed_dates() {
        // Threshold: 2020-06-01
        let rule = DateBoundaryCheck::new(false, 2020, Some(6), Some(1)).unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 5, 31)),  // Before (valid)
            Some(date_to_days(2020, 6, 2)),   // After (violation)
            Some(date_to_days(2019, 1, 1)),   // Before (valid)
            Some(date_to_days(2021, 12, 31)), // After (violation)
            Some(date_to_days(2020, 1, 1)),   // Before (valid)
        ]);

        // 2 violations: 2020-06-02 and 2021-12-31
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_validate_before_date_equal_to_threshold() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(false, 2020, Some(1), Some(1)).unwrap();

        // Date exactly equal to threshold
        let array = Date32Array::from(vec![
            Some(date_to_days(2020, 1, 1)), // Equal (should be violation with >=)
        ]);

        // Date equal to threshold counts as violation (not strictly before)
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_validate_before_with_nulls() {
        // Threshold: 2020-12-31
        let rule = DateBoundaryCheck::new(false, 2020, Some(12), Some(31)).unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(2021, 1, 1)),  // After (violation)
            None,                            // Null (ignored)
            Some(date_to_days(2020, 6, 15)), // Before (valid)
            None,                            // Null (ignored)
            Some(date_to_days(2021, 6, 15)), // After (violation)
        ]);

        // 2 violations, nulls are ignored
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    // ============================================================================
    // Edge Case Tests
    // ============================================================================

    #[test]
    fn test_validate_empty_array() {
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();
        let array = Date32Array::from(Vec::<Option<i32>>::new());

        // Empty array has no violations
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_all_nulls() {
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();
        let array = Date32Array::from(vec![None, None, None, None]);

        // All nulls are ignored, no violations
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_single_value_valid() {
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();
        let array = Date32Array::from(vec![Some(date_to_days(2020, 6, 15))]);

        // Single date after threshold - no violation
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_single_value_violation() {
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();
        let array = Date32Array::from(vec![Some(date_to_days(2019, 6, 15))]);

        // Single date before threshold - 1 violation
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_validate_large_dataset() {
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();

        // Create array with 1000 dates: half before, half after
        let mut dates = Vec::new();
        for i in 0..500 {
            dates.push(Some(date_to_days(2019, 1, 1) - i)); // Before threshold
        }
        for i in 0..500 {
            dates.push(Some(date_to_days(2020, 1, 2) + i)); // After threshold
        }

        let array = Date32Array::from(dates);

        // 500 violations (all the dates before threshold)
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 500);
    }

    #[test]
    fn test_rule_name() {
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();
        assert_eq!(rule.name(), "DateBoundaryCheck");
    }

    #[test]
    fn test_dates_around_epoch() {
        let rule = DateBoundaryCheck::new(true, 1970, Some(1), Some(1)).unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(1969, 12, 31)), // Before epoch (violation)
            Some(date_to_days(1970, 1, 1)),   // Epoch itself (violation, equal)
            Some(date_to_days(1970, 1, 2)),   // After epoch (valid)
        ]);

        // 2 violations: 1969-12-31 and 1970-01-01 (equal)
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_year_boundaries() {
        // Test year boundary crossing
        let rule = DateBoundaryCheck::new(true, 2020, Some(1), Some(1)).unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(2019, 12, 31)), // Last day of 2019 (violation)
            Some(date_to_days(2020, 1, 1)),   // First day of 2020 (violation, equal)
            Some(date_to_days(2020, 1, 2)),   // Second day of 2020 (valid)
        ]);

        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }
}
