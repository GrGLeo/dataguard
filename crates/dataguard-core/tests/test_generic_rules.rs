use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use dataguard_core::rules::generic::{TypeCheck, UnicityCheck};

#[test]
fn test_type_check_valid_string_array() {
    let rule = TypeCheck::new("col".to_string(), DataType::Utf8);
    let array = StringArray::from(vec![Some("hello"), Some("world"), None]);
    let array_ref: &dyn Array = &array;
    let result = rule.validate(array_ref);
    assert!(result.is_ok());
    let (errors, _casted) = result.unwrap();
    assert_eq!(errors, 0); // All strings, no errors
}

#[test]
fn test_type_check_invalid_type() {
    let rule = TypeCheck::new("col".to_string(), DataType::Int32);
    let array = StringArray::from(vec![Some("hello"), Some("world"), None]);
    let array_ref: &dyn Array = &array;
    let result = rule.validate(array_ref);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().0, 2);
}

#[test]
fn test_type_check_mixed_type() {
    let rule = TypeCheck::new("col".to_string(), DataType::Int32);
    let array = StringArray::from(vec![Some("hello"), Some("32"), None]);
    let array_ref: &dyn Array = &array;
    let result = rule.validate(array_ref);
    assert!(result.is_ok());
    let (errors, _casted) = result.unwrap();
    assert_eq!(errors, 1);
}

#[test]
fn test_unicity_check_all_unique() {
    let rule = UnicityCheck::new();
    let array = StringArray::from(vec![
        Some("apple"),
        Some("banana"),
        Some("cherry"),
        Some("date"),
    ]);
    let (_null_count, hash_set) = rule.validate_str(&array);
    assert_eq!(hash_set.len(), 4); // All unique
}

#[test]
fn test_unicity_check_with_duplicates() {
    let rule = UnicityCheck::new();
    let array = StringArray::from(vec![
        Some("apple"),
        Some("banana"),
        Some("apple"),
        Some("cherry"),
        Some("banana"),
    ]);
    let (_null_count, hash_set) = rule.validate_str(&array);
    assert_eq!(hash_set.len(), 3); // Only 3 unique values
}

#[test]
fn test_unicity_check_with_nulls() {
    let rule = UnicityCheck::new();
    let array = StringArray::from(vec![
        Some("apple"),
        None,
        Some("banana"),
        None,
        Some("apple"),
    ]);
    let (null_count, hash_set) = rule.validate_str(&array);
    // Nulls are ignored, only "apple" and "banana" are unique
    assert_eq!(null_count, 2);
    assert_eq!(hash_set.len(), 2);
}

#[test]
fn test_unicity_check_all_nulls() {
    let rule = UnicityCheck::new();
    let null_vec: Vec<Option<&str>> = vec![None, None, None];
    let array = StringArray::from(null_vec);
    let (null_count, hash_set) = rule.validate_str(&array);
    assert_eq!(null_count, 3);
    assert_eq!(hash_set.len(), 0); // No non-null values
}

#[test]
fn test_unicity_check_empty_array() {
    let rule = UnicityCheck::new();
    let empty_vec: Vec<Option<&str>> = vec![];
    let array = StringArray::from(empty_vec);
    let (null_count, hash_set) = rule.validate_str(&array);
    assert_eq!(null_count, 0);
    assert_eq!(hash_set.len(), 0); // Empty
}

#[test]
fn test_unicity_check_single_value() {
    let rule = UnicityCheck::new();
    let array = StringArray::from(vec![Some("only")]);
    let (_null_count, hash_set) = rule.validate_str(&array);
    assert_eq!(hash_set.len(), 1);
}

#[test]
fn test_unicity_check_case_sensitive() {
    let rule = UnicityCheck::new();
    let array = StringArray::from(vec![Some("Apple"), Some("apple"), Some("APPLE")]);
    let (_null_count, hash_set) = rule.validate_str(&array);
    // Case-sensitive, all different
    assert_eq!(hash_set.len(), 3);
}

#[test]
fn test_unicity_check_empty_strings() {
    let rule = UnicityCheck::new();
    let array = StringArray::from(vec![Some(""), Some("a"), Some(""), Some("b")]);
    let (_null_count, hash_set) = rule.validate_str(&array);
    // Empty string is a valid value: "", "a", "b"
    assert_eq!(hash_set.len(), 3);
}
