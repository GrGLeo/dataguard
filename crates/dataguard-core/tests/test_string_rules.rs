use arrow::array::StringArray;
use dataguard_core::rules::string::{IsInCheck, RegexMatch, StringLengthCheck, StringRule};

#[test]
fn test_string_length_check_with_nulls() {
    let rule = StringLengthCheck::new("string_length_test".to_string(), 0.0, Some(3), Some(10));
    let array = StringArray::from(vec![
        Some("abc"),
        None,
        Some("abcdefghij"),
        Some("ab"),
        Some("abcdefghijk"),
    ]);
    // Errors: "ab" (too short), "abcdefghijk" (too long) = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_string_length_exact() {
    let rule = StringLengthCheck::new("string_length_test".to_string(), 0.0, Some(5), Some(5));
    let array = StringArray::from(vec![
        Some("hello"),
        Some("world"),
        Some("hi"),
        Some("toolong"),
    ]);
    // Errors: "hi" (too short), "toolong" (too long) = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_regex_match_email() {
    let rule = RegexMatch::new(
        "regex_match_test".to_string(),
        0.0,
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$".to_string(),
        None,
    );
    let array = StringArray::from(vec![
        Some("test@example.com"),
        Some("invalid-email"),
        Some("another@test.co.uk"),
        Some("@invalid.com"),
        None,
    ]);
    // Errors: "invalid-email", "@invalid.com" = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_regex_match_case_insensitive() {
    let rule = RegexMatch::new(
        "regex_match_test".to_string(),
        0.0,
        "^HELLO$".to_string(),
        Some("i".to_string()),
    );
    let array = StringArray::from(vec![
        Some("hello"),
        Some("HELLO"),
        Some("Hello"),
        Some("hELLo"),
        Some("goodbye"),
    ]);
    // Only "goodbye" should fail
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_is_in_check_with_duplicates() {
    let members = vec!["a".to_string(), "b".to_string(), "a".to_string()]; // "a" appears twice
    let rule = IsInCheck::new("is_in_test".to_string(), 0.0, members);
    let array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
    // "c" is not in members
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_is_in_check_all_valid() {
    let members = vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
    ];
    let rule = IsInCheck::new("is_in_test".to_string(), 0.0, members);
    let array = StringArray::from(vec![
        Some("apple"),
        Some("banana"),
        Some("cherry"),
        Some("apple"),
    ]);
    // All values are in members
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 0);
}

#[test]
fn test_is_in_check_all_invalid() {
    let members = vec!["apple".to_string()];
    let rule = IsInCheck::new("is_in_test".to_string(), 0.0, members);
    let array = StringArray::from(vec![Some("banana"), Some("cherry"), Some("orange"), None]);
    // All values are invalid (excluding None)
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 3);
}

#[test]
fn test_string_length_no_min() {
    let rule = StringLengthCheck::new("string_length_test".to_string(), 0.0, None, Some(5));
    let array = StringArray::from(vec![Some(""), Some("a"), Some("12345"), Some("123456")]);
    // Only "123456" exceeds max
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 1);
}

#[test]
fn test_string_length_no_max() {
    let rule = StringLengthCheck::new("string_length_test".to_string(), 0.0, Some(5), None);
    let array = StringArray::from(vec![
        Some(""),
        Some("abcd"),
        Some("abcde"),
        Some("abcdefghij"),
    ]);
    // "", "abcd" are below min = 2 errors
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}

#[test]
fn test_regex_match_url_pattern() {
    let rule = RegexMatch::new(
        "regex_match_test".to_string(),
        0.0,
        r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}".to_string(),
        None,
    );
    let array = StringArray::from(vec![
        Some("https://example.com"),
        Some("http://test.org"),
        Some("ftp://invalid.com"),
        Some("not-a-url"),
        Some("https://valid.co.uk"),
    ]);
    // Errors: "ftp://invalid.com", "not-a-url" = 2
    assert_eq!(rule.validate(&array, "col".to_string()).unwrap(), 2);
}
