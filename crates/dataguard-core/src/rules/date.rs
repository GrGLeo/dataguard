use arrow::datatypes::DataType;
use arrow_array::{Array, Date32Array, StringArray};
use chrono::{Datelike, Duration, NaiveDate, Weekday};

use crate::{utils::date_parser::parse_date_column, RuleError};

pub struct DateTypeCheck {
    // Those two field are not needed now as we dont need the expected
    // datatype this will be used later on when we also handle Date64Type
    _column: String,
    _expected: DataType,
    threshold: f64,
    format: String,
}

impl DateTypeCheck {
    pub fn new(column: String, expected: DataType, format: String, threshold: f64) -> Self {
        Self {
            _column: column,
            _expected: expected,
            threshold,
            format,
        }
    }

    pub fn name(&self) -> String {
        "TypeCheck".to_string()
    }

    pub fn get_threshold(&self) -> f64 {
        self.threshold
    }

    pub fn validate(&self, array: &dyn Array) -> Result<(usize, Date32Array), RuleError> {
        let base_nulls = array.null_count();
        // We know that we pass in a string array given that we parse all incoming columns as
        // StringArray so we can unwrap safely
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        let casted_array = parse_date_column(array, &self.format);
        let errors = casted_array.null_count() - base_nulls;
        Ok((errors, casted_array))
    }
}

/// A trait for defining validation rules on Arrow arrays.
pub trait DateRule: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> String;
    /// Returns the rule threshold
    fn get_threshold(&self) -> f64;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &Date32Array, column: String) -> Result<usize, RuleError>;
}

pub struct DateBoundaryCheck {
    name: String,
    threshold: f64,
    days: i32,
    after: bool,
}

impl DateBoundaryCheck {
    pub fn new(
        name: String,
        threshold: f64,
        after: bool,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
    ) -> Result<Self, RuleError> {
        let m = month.unwrap_or(1) as u32;
        let d = day.unwrap_or(1) as u32;
        let res = NaiveDate::from_ymd_opt(year as i32, m, d);
        match res {
            Some(date) => {
                // Here we can unwrap date is correct
                let unix = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = date.signed_duration_since(unix).num_days() as i32;
                Ok(Self {
                    name,
                    threshold,
                    after,
                    days,
                })
            }
            None => Err(RuleError::IncorrectDateError(year, m, d)),
        }
    }
}

impl DateRule for DateBoundaryCheck {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_threshold(&self) -> f64 {
        self.threshold
    }

    fn validate(&self, array: &Date32Array, _column: String) -> Result<usize, RuleError> {
        let mut counter = 0;
        for day in array.iter().flatten() {
            if self.after {
                if day <= self.days {
                    counter += 1;
                }
            } else if day >= self.days {
                counter += 1;
            }
        }
        Ok(counter)
    }
}

pub struct WeekDayCheck {
    name: String,
    threshold: f64,
    is_week: bool,
}

impl WeekDayCheck {
    pub fn new(name: String, threshold: f64, is_week: bool) -> Self {
        Self {
            name,
            threshold,
            is_week,
        }
    }
}

impl Default for WeekDayCheck {
    fn default() -> Self {
        Self::new("IsWeekday".to_string(), 0., true)
    }
}

impl DateRule for WeekDayCheck {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_threshold(&self) -> f64 {
        self.threshold
    }

    fn validate(&self, array: &Date32Array, _column: String) -> Result<usize, RuleError> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let mut violations = 0;

        for day in array.iter().flatten() {
            let date = epoch + Duration::days(day as i64);
            match date.weekday() {
                Weekday::Sun | Weekday::Sat => {
                    if self.is_week {
                        violations += 1
                    }
                }
                _ => {
                    if !self.is_week {
                        violations += 1
                    }
                }
            }
        }
        Ok(violations)
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
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(6),
            Some(15),
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, date_to_days(2020, 6, 15));
        assert_eq!(rule.after, true);
    }

    #[test]
    fn test_constructor_valid_date_year_and_month() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            false,
            2020,
            Some(6),
            None,
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        // Should default to day 1
        assert_eq!(rule.days, date_to_days(2020, 6, 1));
        assert_eq!(rule.after, false);
    }

    #[test]
    fn test_constructor_valid_date_year_only() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            None,
            None,
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        // Should default to January 1st
        assert_eq!(rule.days, date_to_days(2020, 1, 1));
    }

    #[test]
    fn test_constructor_invalid_month() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(13),
            Some(1),
        );
        assert!(result.is_err());
        match result {
            Err(RuleError::IncorrectDateError(year, month, day)) => {
                assert_eq!(year, 2020);
                assert_eq!(month, 13);
                assert_eq!(day, 1);
            }
            _ => panic!("Expected IncorrectDate error"),
        }
    }

    #[test]
    fn test_constructor_invalid_day() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(2),
            Some(30),
        );
        assert!(result.is_err());
        match result {
            Err(RuleError::IncorrectDateError(year, month, day)) => {
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
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(2),
            Some(29),
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, date_to_days(2020, 2, 29));
    }

    #[test]
    fn test_constructor_non_leap_year_feb_29() {
        // 2021 is not a leap year, Feb 29 should be invalid
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2021,
            Some(2),
            Some(29),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_constructor_unix_epoch() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            1970,
            Some(1),
            Some(1),
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, 0); // Unix epoch is day 0
    }

    #[test]
    fn test_constructor_date_before_epoch() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            1969,
            Some(12),
            Some(31),
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, -1); // Day before epoch
    }

    #[test]
    fn test_constructor_far_future_date() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2100,
            Some(1),
            Some(1),
        );
        assert!(result.is_ok());
        let rule = result.unwrap();
        assert_eq!(rule.days, date_to_days(2100, 1, 1));
    }

    #[test]
    fn test_constructor_month_zero_invalid() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(0),
            Some(1),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_constructor_day_zero_invalid() {
        let result = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(0),
        );
        assert!(result.is_err());
    }

    // ============================================================================
    // Validation Tests - After (dates should be AFTER threshold)
    // ============================================================================

    #[test]
    fn test_validate_after_all_dates_after_threshold() {
        // Threshold: 2020-01-01
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(6),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            false,
            2020,
            Some(12),
            Some(31),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            false,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            false,
            2020,
            Some(6),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            false,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            false,
            2020,
            Some(12),
            Some(31),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();
        let array = Date32Array::from(Vec::<Option<i32>>::new());

        // Empty array has no violations
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_all_nulls() {
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();
        let array = Date32Array::from(vec![None, None, None, None]);

        // All nulls are ignored, no violations
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_single_value_valid() {
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();
        let array = Date32Array::from(vec![Some(date_to_days(2020, 6, 15))]);

        // Single date after threshold - no violation
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_validate_single_value_violation() {
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();
        let array = Date32Array::from(vec![Some(date_to_days(2019, 6, 15))]);

        // Single date before threshold - 1 violation
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_validate_large_dataset() {
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();
        assert_eq!(rule.name(), "date_boundary_test");
    }

    #[test]
    fn test_dates_around_epoch() {
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            1970,
            Some(1),
            Some(1),
        )
        .unwrap();

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
        let rule = DateBoundaryCheck::new(
            "date_boundary_test".to_string(),
            0.0,
            true,
            2020,
            Some(1),
            Some(1),
        )
        .unwrap();

        let array = Date32Array::from(vec![
            Some(date_to_days(2019, 12, 31)), // Last day of 2019 (violation)
            Some(date_to_days(2020, 1, 1)),   // First day of 2020 (violation, equal)
            Some(date_to_days(2020, 1, 2)),   // Second day of 2020 (valid)
        ]);

        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    // ============================================================================
    // WeekDayCheck Tests - is_week = true (weekends are violations)
    // ============================================================================

    #[test]
    fn test_weekday_check_all_weekdays_iso_format() {
        // Happy path: All dates are weekdays (Monday-Friday)
        // Using ISO format: %Y-%m-%d
        let rule = WeekDayCheck::default(); // is_week = true

        // 2025-01-06 = Monday
        // 2025-01-07 = Tuesday
        // 2025-01-08 = Wednesday
        // 2025-01-09 = Thursday
        // 2025-01-10 = Friday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 6)),  // Monday
            Some(date_to_days(2025, 1, 7)),  // Tuesday
            Some(date_to_days(2025, 1, 8)),  // Wednesday
            Some(date_to_days(2025, 1, 9)),  // Thursday
            Some(date_to_days(2025, 1, 10)), // Friday
        ]);

        // No violations - all dates are weekdays
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_weekday_check_all_weekdays_us_format() {
        // Happy path: All dates are weekdays (Monday-Friday)
        // Using US format: %m/%d/%Y (conceptually - we're testing with Date32Array)
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, true); // is_week = true

        // Different week to show format independence
        // 2025-01-13 = Monday
        // 2025-01-14 = Tuesday
        // 2025-01-15 = Wednesday
        // 2025-01-16 = Thursday
        // 2025-01-17 = Friday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 13)), // Monday
            Some(date_to_days(2025, 1, 14)), // Tuesday
            Some(date_to_days(2025, 1, 15)), // Wednesday
            Some(date_to_days(2025, 1, 16)), // Thursday
            Some(date_to_days(2025, 1, 17)), // Friday
        ]);

        // No violations - all dates are weekdays
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_weekday_check_with_weekend_iso_format() {
        // Failing path: Contains weekend dates
        // Using ISO format: %Y-%m-%d
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, true); // is_week = true

        // 2025-01-06 = Monday
        // 2025-01-11 = Saturday (violation)
        // 2025-01-08 = Wednesday
        // 2025-01-12 = Sunday (violation)
        // 2025-01-10 = Friday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 6)),  // Monday (ok)
            Some(date_to_days(2025, 1, 11)), // Saturday (violation)
            Some(date_to_days(2025, 1, 8)),  // Wednesday (ok)
            Some(date_to_days(2025, 1, 12)), // Sunday (violation)
            Some(date_to_days(2025, 1, 10)), // Friday (ok)
        ]);

        // 2 violations: Saturday and Sunday
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_weekday_check_with_weekend_us_format() {
        // Failing path: Contains weekend dates
        // Using US format: %m/%d/%Y (conceptually)
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, true); // is_week = true

        // Different dates
        // 2025-01-13 = Monday
        // 2025-01-18 = Saturday (violation)
        // 2025-01-15 = Wednesday
        // 2025-01-19 = Sunday (violation)
        // 2025-01-17 = Friday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 13)), // Monday (ok)
            Some(date_to_days(2025, 1, 18)), // Saturday (violation)
            Some(date_to_days(2025, 1, 15)), // Wednesday (ok)
            Some(date_to_days(2025, 1, 19)), // Sunday (violation)
            Some(date_to_days(2025, 1, 17)), // Friday (ok)
        ]);

        // 2 violations: Saturday and Sunday
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    // ============================================================================
    // WeekDayCheck Tests - is_week = false (weekdays are violations)
    // ============================================================================

    #[test]
    fn test_weekend_check_all_weekends_iso_format() {
        // Happy path: All dates are weekends (Saturday-Sunday)
        // Using ISO format: %Y-%m-%d
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, false); // is_week = false

        // 2025-01-04 = Saturday
        // 2025-01-05 = Sunday
        // 2025-01-11 = Saturday
        // 2025-01-12 = Sunday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 4)),  // Saturday
            Some(date_to_days(2025, 1, 5)),  // Sunday
            Some(date_to_days(2025, 1, 11)), // Saturday
            Some(date_to_days(2025, 1, 12)), // Sunday
        ]);

        // No violations - all dates are weekends
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_weekend_check_all_weekends_us_format() {
        // Happy path: All dates are weekends (Saturday-Sunday)
        // Using US format: %m/%d/%Y (conceptually)
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, false); // is_week = false

        // Different dates
        // 2025-01-18 = Saturday
        // 2025-01-19 = Sunday
        // 2025-01-25 = Saturday
        // 2025-01-26 = Sunday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 18)), // Saturday
            Some(date_to_days(2025, 1, 19)), // Sunday
            Some(date_to_days(2025, 1, 25)), // Saturday
            Some(date_to_days(2025, 1, 26)), // Sunday
        ]);

        // No violations - all dates are weekends
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_weekend_check_with_weekdays_iso_format() {
        // Failing path: Contains weekday dates
        // Using ISO format: %Y-%m-%d
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, false); // is_week = false

        // 2025-01-04 = Saturday
        // 2025-01-06 = Monday (violation)
        // 2025-01-05 = Sunday
        // 2025-01-08 = Wednesday (violation)
        // 2025-01-11 = Saturday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 4)),  // Saturday (ok)
            Some(date_to_days(2025, 1, 6)),  // Monday (violation)
            Some(date_to_days(2025, 1, 5)),  // Sunday (ok)
            Some(date_to_days(2025, 1, 8)),  // Wednesday (violation)
            Some(date_to_days(2025, 1, 11)), // Saturday (ok)
        ]);

        // 2 violations: Monday and Wednesday
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_weekend_check_with_weekdays_us_format() {
        // Failing path: Contains weekday dates
        // Using US format: %m/%d/%Y (conceptually)
        let rule = WeekDayCheck::new("weekday_test".to_string(), 0.0, false); // is_week = false

        // Different dates
        // 2025-01-18 = Saturday
        // 2025-01-20 = Monday (violation)
        // 2025-01-19 = Sunday
        // 2025-01-22 = Wednesday (violation)
        // 2025-01-25 = Saturday
        let array = Date32Array::from(vec![
            Some(date_to_days(2025, 1, 18)), // Saturday (ok)
            Some(date_to_days(2025, 1, 20)), // Monday (violation)
            Some(date_to_days(2025, 1, 19)), // Sunday (ok)
            Some(date_to_days(2025, 1, 22)), // Wednesday (violation)
            Some(date_to_days(2025, 1, 25)), // Saturday (ok)
        ]);

        // 2 violations: Monday and Wednesday
        assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
    }
}
