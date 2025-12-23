#[cfg(test)]
use crate::columns::{
    date_builder::DateColumnBuilder, numeric_builder::NumericColumnBuilder,
    string_builder::StringColumnBuilder, ColumnBuilder, ColumnRule, ColumnType,
};

#[test]
fn test_string_column_builder() {
    let mut builder = StringColumnBuilder::new("name".to_string());
    builder.with_min_length(3).with_max_length(50);

    assert_eq!(builder.name(), "name");
    assert_eq!(builder.column_type(), ColumnType::String);
    assert_eq!(builder.rules().len(), 2);
}

#[test]
fn test_string_column_with_regex() {
    let mut builder = StringColumnBuilder::new("email".to_string());
    builder.is_email().unwrap();

    assert_eq!(builder.column_type(), ColumnType::String);
    assert_eq!(builder.rules().len(), 1);
    match &builder.rules()[0] {
        ColumnRule::StringRegex { pattern, .. } => {
            assert!(pattern.contains("@"));
        }
        _ => panic!("Expected StringRegex rule"),
    }
}

#[test]
fn test_string_column_invalid_regex() {
    let mut builder = StringColumnBuilder::new("test".to_string());
    let result = builder.with_regex("[invalid(".to_string(), None);

    assert!(result.is_err());
}

#[test]
fn test_integer_column_builder() {
    let mut builder = NumericColumnBuilder::<i64>::new("age".to_string());
    builder.between(0, 120);

    assert_eq!(builder.name(), "age");
    assert_eq!(builder.column_type(), ColumnType::Integer);
    assert_eq!(builder.rules().len(), 1);
}

#[test]
fn test_integer_column_is_positive() {
    let mut builder = NumericColumnBuilder::<i64>::new("count".to_string());
    builder.is_positive();

    match &builder.rules()[0] {
        ColumnRule::NumericRange { min, max, .. } => {
            assert_eq!(min, &Some(1.0));
            assert_eq!(max, &None);
        }
        _ => panic!("Expected NumericRange rule"),
    }
}

#[test]
fn test_float_column_builder() {
    let mut builder = NumericColumnBuilder::<f64>::new("price".to_string());
    builder.between(0.0, 1000.0);

    assert_eq!(builder.name(), "price");
    assert_eq!(builder.column_type(), ColumnType::Float);
    assert_eq!(builder.rules().len(), 1);
}

#[test]
fn test_float_column_monotonicity() {
    let mut builder = NumericColumnBuilder::<f64>::new("timestamp".to_string());
    builder.is_monotonically_increasing();

    match &builder.rules()[0] {
        ColumnRule::Monotonicity { ascending, .. } => {
            assert!(ascending);
        }
        _ => panic!("Expected Monotonicity rule"),
    }
}

#[test]
fn test_column_chaining() {
    let mut builder = StringColumnBuilder::new("username".to_string());
    builder
        .with_min_length(3)
        .with_max_length(20)
        .is_alphanumeric()
        .unwrap()
        .is_unique();

    assert_eq!(builder.rules().len(), 4);
}

// ============================================================================
// Date Column Builder Tests
// ============================================================================

#[test]
fn test_date_column_builder() {
    let mut builder = DateColumnBuilder::new("created_at".to_string(), "%Y-%m-%d".to_string());
    builder.is_after(2020, Some(1), Some(1));

    assert_eq!(builder.name(), "created_at");
    assert_eq!(builder.column_type(), ColumnType::DateType);
    assert_eq!(builder.rules().len(), 1);
}

#[test]
fn test_date_column_is_after_full_date() {
    let mut builder = DateColumnBuilder::new("event_date".to_string(), "%Y-%m-%d".to_string());
    builder.is_after(2020, Some(6), Some(15));

    assert_eq!(builder.rules().len(), 1);
    match &builder.rules()[0] {
        ColumnRule::DateBoundary {
            after,
            year,
            month,
            day,
            ..
        } => {
            assert_eq!(after, &true);
            assert_eq!(year, &2020);
            assert_eq!(month, &Some(6));
            assert_eq!(day, &Some(15));
        }
        _ => panic!("Expected DateBoundary rule"),
    }
}

#[test]
fn test_date_column_is_after_year_and_month() {
    let mut builder = DateColumnBuilder::new("event_date".to_string(), "%Y-%m-%d".to_string());
    builder.is_after(2020, Some(6), None);

    match &builder.rules()[0] {
        ColumnRule::DateBoundary {
            after,
            year,
            month,
            day,
            ..
        } => {
            assert_eq!(after, &true);
            assert_eq!(year, &2020);
            assert_eq!(month, &Some(6));
            assert_eq!(day, &None); // Day is None
        }
        _ => panic!("Expected DateBoundary rule"),
    }
}

#[test]
fn test_date_column_is_after_year_only() {
    let mut builder = DateColumnBuilder::new("event_date".to_string(), "%Y-%m-%d".to_string());
    builder.is_after(2020, None, None);

    match &builder.rules()[0] {
        ColumnRule::DateBoundary {
            after,
            year,
            month,
            day,
            ..
        } => {
            assert_eq!(after, &true);
            assert_eq!(year, &2020);
            assert_eq!(month, &None);
            assert_eq!(day, &None);
        }
        _ => panic!("Expected DateBoundary rule"),
    }
}

#[test]
fn test_date_column_is_before_full_date() {
    let mut builder = DateColumnBuilder::new("expiry_date".to_string(), "%Y-%m-%d".to_string());
    builder.is_before(2025, Some(12), Some(31));

    assert_eq!(builder.rules().len(), 1);
    match &builder.rules()[0] {
        ColumnRule::DateBoundary {
            after,
            year,
            month,
            day,
            ..
        } => {
            assert_eq!(after, &false);
            assert_eq!(year, &2025);
            assert_eq!(month, &Some(12));
            assert_eq!(day, &Some(31));
        }
        _ => panic!("Expected DateBoundary rule"),
    }
}

#[test]
fn test_date_column_is_before_year_and_month() {
    let mut builder = DateColumnBuilder::new("deadline".to_string(), "%Y-%m-%d".to_string());
    builder.is_before(2024, Some(3), None);

    match &builder.rules()[0] {
        ColumnRule::DateBoundary {
            after,
            year,
            month,
            day,
            ..
        } => {
            assert_eq!(after, &false);
            assert_eq!(year, &2024);
            assert_eq!(month, &Some(3));
            assert_eq!(day, &None);
        }
        _ => panic!("Expected DateBoundary rule"),
    }
}

#[test]
fn test_date_column_is_before_year_only() {
    let mut builder = DateColumnBuilder::new("deadline".to_string(), "%Y-%m-%d".to_string());
    builder.is_before(2030, None, None);

    match &builder.rules()[0] {
        ColumnRule::DateBoundary {
            after,
            year,
            month,
            day,
            ..
        } => {
            assert_eq!(after, &false);
            assert_eq!(year, &2030);
            assert_eq!(month, &None);
            assert_eq!(day, &None);
        }
        _ => panic!("Expected DateBoundary rule"),
    }
}

#[test]
fn test_date_column_is_not_null() {
    let mut builder = DateColumnBuilder::new("birth_date".to_string(), "%Y-%m-%d".to_string());
    builder.is_not_null();

    assert_eq!(builder.rules().len(), 1);
    match &builder.rules()[0] {
        ColumnRule::NullCheck => {}
        _ => panic!("Expected NullCheck rule"),
    }
}

#[test]
fn test_date_column_is_unique() {
    let mut builder =
        DateColumnBuilder::new("transaction_date".to_string(), "%Y-%m-%d".to_string());
    builder.is_unique();

    assert_eq!(builder.rules().len(), 1);
    match &builder.rules()[0] {
        ColumnRule::Unicity => {}
        _ => panic!("Expected Unicity rule"),
    }
}

#[test]
fn test_date_column_chaining() {
    let mut builder = DateColumnBuilder::new("order_date".to_string(), "%Y-%m-%d".to_string());
    builder
        .is_not_null()
        .is_after(2020, Some(1), Some(1))
        .is_before(2030, Some(12), Some(31));

    assert_eq!(builder.rules().len(), 3);

    // Check first rule is NullCheck
    match &builder.rules()[0] {
        ColumnRule::NullCheck => {}
        _ => panic!("Expected NullCheck as first rule"),
    }

    // Check second rule is DateBoundary with after=true
    match &builder.rules()[1] {
        ColumnRule::DateBoundary { after, .. } => {
            assert_eq!(after, &true);
        }
        _ => panic!("Expected DateBoundary as second rule"),
    }

    // Check third rule is DateBoundary with after=false
    match &builder.rules()[2] {
        ColumnRule::DateBoundary { after, .. } => {
            assert_eq!(after, &false);
        }
        _ => panic!("Expected DateBoundary as third rule"),
    }
}

#[test]
fn test_date_column_multiple_boundaries() {
    // Test that you can set both before and after boundaries
    let mut builder = DateColumnBuilder::new("valid_period".to_string(), "%Y-%m-%d".to_string());
    builder
        .is_after(2020, Some(1), Some(1))
        .is_before(2025, Some(12), Some(31))
        .is_unique();

    assert_eq!(builder.rules().len(), 3);
}

#[test]
fn test_date_column_builder_name() {
    let builder = DateColumnBuilder::new("my_date".to_string(), "%Y-%m-%d".to_string());
    assert_eq!(builder.name(), "my_date");
}

#[test]
fn test_date_column_builder_type() {
    let builder = DateColumnBuilder::new("date".to_string(), "%Y-%m-%d".to_string());
    assert_eq!(builder.column_type(), ColumnType::DateType);
}

#[test]
fn test_date_column_empty_rules() {
    let builder = DateColumnBuilder::new("date".to_string(), "%Y-%m-%d".to_string());
    assert_eq!(builder.rules().len(), 0);
}
