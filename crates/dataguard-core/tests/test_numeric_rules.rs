use arrow::array::{Float64Array, Int64Array};
use dataguard_core::rules::numeric::{Monotonicity, NumericRule, Range};

#[test]
fn test_range_float_with_nulls() {
    let rule = Range::new("range_test".to_string(), Some(1.5), Some(10.5));
    let array = Float64Array::from(vec![
        Some(1.5),
        Some(5.0),
        None,
        Some(10.5),
        Some(11.0),
        Some(1.0),
    ]);
    // Errors: 11.0 (> max), 1.0 (< min) = 3
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_range_exact_boundaries() {
    let rule = Range::new("range_test".to_string(), Some(5i64), Some(5i64));
    let array = Int64Array::from(vec![Some(4), Some(5), Some(6)]);
    // Errors: 4 (< min), 6 (> max) = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_monotonicity_single_element() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(vec![42]);
    // Single element is always monotonic
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_empty_array() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(Vec::<i64>::new());
    // Empty array is always monotonic
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_with_floats() {
    let rule = Monotonicity::<f64>::new("monotonicity_test".to_string(), true);
    let array = Float64Array::from(vec![1.0, 2.5, 2.5, 3.7, 5.1]);
    // All ascending or equal
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_float_descending() {
    let rule = Monotonicity::<f64>::new("monotonicity_test".to_string(), false);
    let array = Float64Array::from(vec![10.5, 8.3, 8.3, 5.1, 2.0]);
    // All descending or equal
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_range_all_valid_integers() {
    let rule = Range::new("range_test".to_string(), Some(0i64), Some(100i64));
    let array = Int64Array::from(vec![0, 25, 50, 75, 100]);
    // All values within range
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_range_all_invalid_integers() {
    let rule = Range::new("range_test".to_string(), Some(10i64), Some(20i64));
    let array = Int64Array::from(vec![5, 25, 30, 0, 100]);
    // All values outside range
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 5);
}

#[test]
fn test_range_negative_floats() {
    let rule = Range::new("range_test".to_string(), Some(-10.0), Some(-1.0));
    let array = Float64Array::from(vec![Some(-5.5), Some(-0.5), Some(-10.5), Some(-9.9), None]);
    // Errors: -0.5 (> max), -10.5 (< min) = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_monotonicity_strict_ascending_violation() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(vec![1, 5, 10, 15, 14, 20]);
    // 14 < 15, so one violation
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_monotonicity_strict_descending_violation() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), false);
    let array = Int64Array::from(vec![20, 15, 10, 11, 5]);
    // 11 > 10, so one violation
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_range_zero_boundary() {
    let rule = Range::new("range_test".to_string(), Some(0i64), None);
    let array = Int64Array::from(vec![Some(-1), Some(0), Some(1), None]);
    // Errors: -1 (< 0) = 1
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

// ============================================================================
// Monotonicity with Nulls Tests - Current Behavior
// ============================================================================
// NOTE: See MONOTONICITY_NULL_HANDLING.md for discussion on whether this
// behavior is correct. Currently, nulls are IGNORED in monotonicity checks.

#[test]
fn test_monotonicity_with_null_in_middle_ascending() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(vec![Some(1), None, Some(3)]);
    // Current behavior: nulls are ignored, non-null sequence is monotonic
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_with_null_at_start_ascending() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(vec![None, Some(1), Some(2), Some(3)]);
    // Current behavior: null at start is ignored
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_with_null_at_end_ascending() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(vec![Some(1), Some(2), Some(3), None]);
    // Current behavior: null at end is ignored
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_with_multiple_nulls_ascending() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let array = Int64Array::from(vec![Some(1), None, Some(2), None, Some(3)]);
    // Current behavior: all nulls ignored, non-null values (1,2,3) are monotonic
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_all_nulls() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), true);
    let null_vec: Vec<Option<i64>> = vec![None, None, None];
    let array = Int64Array::from(null_vec);
    // Current behavior: all nulls ignored, no comparisons fail
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_with_null_descending() {
    let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), false);
    let array = Int64Array::from(vec![Some(5), None, Some(3), None, Some(1)]);
    // Current behavior: nulls ignored, non-null values (5,3,1) are monotonically descending
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}
