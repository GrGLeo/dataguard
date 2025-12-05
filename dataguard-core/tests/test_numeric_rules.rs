use arrow::array::{Float64Array, Int64Array};
use dataguard_core::rules::numeric::{Monotonicity, NumericRule, Range};

#[test]
fn test_range_float_with_nulls() {
    let rule = Range::new(Some(1.5), Some(10.5));
    let array = Float64Array::from(vec![
        Some(1.5),
        Some(5.0),
        None,
        Some(10.5),
        Some(11.0),
        Some(1.0),
    ]);
    // Errors: None, 11.0 (> max), 1.0 (< min) = 3
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 3);
}

#[test]
fn test_range_exact_boundaries() {
    let rule = Range::new(Some(5i64), Some(5i64));
    let array = Int64Array::from(vec![Some(4), Some(5), Some(6)]);
    // Errors: 4 (< min), 6 (> max) = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_monotonicity_single_element() {
    let rule = Monotonicity::<i64>::new(true);
    let array = Int64Array::from(vec![42]);
    // Single element is always monotonic
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_empty_array() {
    let rule = Monotonicity::<i64>::new(true);
    let array = Int64Array::from(Vec::<i64>::new());
    // Empty array is always monotonic
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_with_floats() {
    let rule = Monotonicity::<f64>::new(true);
    let array = Float64Array::from(vec![1.0, 2.5, 2.5, 3.7, 5.1]);
    // All ascending or equal
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_monotonicity_float_descending() {
    let rule = Monotonicity::<f64>::new(false);
    let array = Float64Array::from(vec![10.5, 8.3, 8.3, 5.1, 2.0]);
    // All descending or equal
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_range_all_valid_integers() {
    let rule = Range::new(Some(0i64), Some(100i64));
    let array = Int64Array::from(vec![0, 25, 50, 75, 100]);
    // All values within range
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_range_all_invalid_integers() {
    let rule = Range::new(Some(10i64), Some(20i64));
    let array = Int64Array::from(vec![5, 25, 30, 0, 100]);
    // All values outside range
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 5);
}

#[test]
fn test_range_negative_floats() {
    let rule = Range::new(Some(-10.0), Some(-1.0));
    let array = Float64Array::from(vec![
        Some(-5.5),
        Some(-0.5),
        Some(-10.5),
        Some(-9.9),
        None,
    ]);
    // Errors: -0.5 (> max), -10.5 (< min), None = 3
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 3);
}

#[test]
fn test_monotonicity_strict_ascending_violation() {
    let rule = Monotonicity::<i64>::new(true);
    let array = Int64Array::from(vec![1, 5, 10, 15, 14, 20]);
    // 14 < 15, so one violation
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_monotonicity_strict_descending_violation() {
    let rule = Monotonicity::<i64>::new(false);
    let array = Int64Array::from(vec![20, 15, 10, 11, 5]);
    // 11 > 10, so one violation
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_range_zero_boundary() {
    let rule = Range::new(Some(0i64), None);
    let array = Int64Array::from(vec![Some(-1), Some(0), Some(1), None]);
    // Errors: -1 (< 0), None = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}
