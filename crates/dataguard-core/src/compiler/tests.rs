use crate::{
    column::{NumericColumnBuilder, StringColumnBuilder},
    compiler::compile_column,
    validator::ExecutableColumn,
};

#[test]
fn test_compile_string_column_basic() {
    let mut builder = StringColumnBuilder::new("username".to_string());
    builder.with_min_length(3).with_max_length(20);

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::String { name, rules, .. } => {
            assert_eq!(name, "username");
            assert_eq!(rules.len(), 2); // Two StringLength rules (min and max)
        }
        _ => panic!("Expected String column"),
    }
}

#[test]
fn test_compile_string_column_with_null_and_unicity() {
    let mut builder = StringColumnBuilder::new("email".to_string());
    builder.is_not_null().is_unique();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::String {
            null_check,
            unicity_check,
            rules,
            ..
        } => {
            assert!(null_check.is_some());
            assert!(unicity_check.is_some());
            assert_eq!(rules.len(), 0); // No domain rules
        }
        _ => panic!("Expected String column"),
    }
}

#[test]
fn test_compile_string_column_with_regex() {
    let mut builder = StringColumnBuilder::new("code".to_string());
    builder.is_alphanumeric().unwrap();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::String { rules, .. } => {
            assert_eq!(rules.len(), 1); // Regex rule
        }
        _ => panic!("Expected String column"),
    }
}

#[test]
fn test_compile_string_column_with_membership() {
    let mut builder = StringColumnBuilder::new("status".to_string());
    builder.is_in(vec!["active".to_string(), "inactive".to_string()]);

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::String { rules, .. } => {
            assert_eq!(rules.len(), 1); // IsIn rule
        }
        _ => panic!("Expected String column"),
    }
}

#[test]
fn test_compile_integer_column_basic() {
    let mut builder = NumericColumnBuilder::<i64>::new("age".to_string());
    builder.between(0, 120);

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Integer { name, rules, .. } => {
            assert_eq!(name, "age");
            assert_eq!(rules.len(), 1); // Range rule
        }
        _ => panic!("Expected Integer column"),
    }
}

#[test]
fn test_compile_integer_column_with_monotonicity() {
    let mut builder = NumericColumnBuilder::<i64>::new("timestamp".to_string());
    builder.is_monotonically_increasing();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Integer { rules, .. } => {
            assert_eq!(rules.len(), 1); // Monotonicity rule
        }
        _ => panic!("Expected Integer column"),
    }
}

#[test]
fn test_compile_integer_column_positive() {
    let mut builder = NumericColumnBuilder::<i64>::new("count".to_string());
    builder.is_positive();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Integer { rules, .. } => {
            assert_eq!(rules.len(), 1); // Range rule (min: 1)
        }
        _ => panic!("Expected Integer column"),
    }
}

#[test]
fn test_compile_float_column_basic() {
    let mut builder = NumericColumnBuilder::<f64>::new("price".to_string());
    builder.between(0.0, 1000.0);

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Float { name, rules, .. } => {
            assert_eq!(name, "price");
            assert_eq!(rules.len(), 1); // Range rule
        }
        _ => panic!("Expected Float column"),
    }
}

#[test]
fn test_compile_float_column_non_negative() {
    let mut builder = NumericColumnBuilder::<f64>::new("amount".to_string());
    builder.is_non_negative();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Float { rules, .. } => {
            assert_eq!(rules.len(), 1); // Range rule (min: 0.0)
        }
        _ => panic!("Expected Float column"),
    }
}

#[test]
fn test_compile_column_with_multiple_rules() {
    let mut builder = StringColumnBuilder::new("username".to_string());
    builder
        .with_min_length(3)
        .with_max_length(20)
        .is_alphanumeric()
        .unwrap()
        .is_unique()
        .is_not_null();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::String {
            rules,
            unicity_check,
            null_check,
            ..
        } => {
            // 3 domain rules: StringLength (min), StringLength (max), Regex
            assert_eq!(rules.len(), 3);
            assert!(unicity_check.is_some());
            assert!(null_check.is_some());
        }
        _ => panic!("Expected String column"),
    }
}

#[test]
fn test_compile_integer_with_unicity_and_null() {
    let mut builder = NumericColumnBuilder::<i64>::new("id".to_string());
    builder.is_unique().is_not_null();

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Integer {
            unicity_check,
            null_check,
            rules,
            ..
        } => {
            assert!(unicity_check.is_some());
            assert!(null_check.is_some());
            assert_eq!(rules.len(), 0); // No domain rules
        }
        _ => panic!("Expected Integer column"),
    }
}

#[test]
fn test_compile_numeric_column_multiple_ranges() {
    let mut builder = NumericColumnBuilder::<i64>::new("value".to_string());
    builder.min(10).max(100);

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Integer { rules, .. } => {
            // Two separate Range rules (one for min, one for max)
            assert_eq!(rules.len(), 2);
        }
        _ => panic!("Expected Integer column"),
    }
}

#[test]
fn test_compile_string_column_type_check_always_present() {
    let builder = StringColumnBuilder::new("test".to_string());

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::String { type_check, .. } => {
            // TypeCheck is always present for CSV compatibility
            assert_eq!(type_check.name(), "TypeCheck");
        }
        _ => panic!("Expected String column"),
    }
}

#[test]
fn test_compile_integer_column_type_check_always_present() {
    let builder = NumericColumnBuilder::<i64>::new("test".to_string());

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Integer { type_check, .. } => {
            // TypeCheck is always present for CSV compatibility
            assert_eq!(type_check.name(), "TypeCheck");
        }
        _ => panic!("Expected Integer column"),
    }
}

#[test]
fn test_compile_float_column_type_check_always_present() {
    let builder = NumericColumnBuilder::<f64>::new("test".to_string());

    let result = compile_column(Box::new(builder));
    assert!(result.is_ok());

    let executable = result.unwrap();
    match executable {
        ExecutableColumn::Float { type_check, .. } => {
            // TypeCheck is always present for CSV compatibility
            assert_eq!(type_check.name(), "TypeCheck");
        }
        _ => panic!("Expected Float column"),
    }
}
