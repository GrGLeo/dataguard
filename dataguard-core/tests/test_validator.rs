use dataguard_core::{
    FloatColumnBuilder, IntegerColumnBuilder, StringColumnBuilder, Validator,
};
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_validator_string_column_validation() {
    // Create a temporary CSV file
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "product_id,description").unwrap();
    writeln!(file, "p1,short").unwrap(); // desc length fail (<6)
    writeln!(file, "p2,a good description").unwrap(); // ok
    writeln!(file, "p3,invalid-char!").unwrap(); // desc regex fail
    writeln!(file, "p4,another good one").unwrap(); // ok
    writeln!(file, "p5,").unwrap(); // desc length fail ("" < 6)

    // Create column rules
    let desc_col = StringColumnBuilder::new("description".to_string())
        .with_regex("^[a-z ]+$".to_string(), None)
        .unwrap()
        .with_min_length(6)
        .build();

    // Commit to validator
    let mut validator = Validator::new();
    validator.commit(vec![desc_col]).unwrap();

    // Run validation
    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    // Expected errors:
    // - "short": fail (length < 6) + pass (regex) = 1 error
    // - "a good description": pass + pass = 0 errors
    // - "invalid-char!": pass (length) + fail (regex has !) = 1 error
    // - "another good one": pass + pass = 0 errors
    // - "": fail (length) + fail (regex) = 2 errors
    assert_eq!(error_count, 4);
}

#[test]
fn test_validator_integer_column_validation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "age,score").unwrap();
    writeln!(file, "25,95").unwrap(); // ok
    writeln!(file, "150,85").unwrap(); // age fail (>120)
    writeln!(file, "-5,75").unwrap(); // age fail (<0)
    writeln!(file, "30,105").unwrap(); // score fail (>100)
    writeln!(file, "45,50").unwrap(); // ok

    let age_col = IntegerColumnBuilder::new("age".to_string())
        .between(Some(0), Some(120))
        .build();

    let score_col = IntegerColumnBuilder::new("score".to_string())
        .between(Some(0), Some(100))
        .build();

    let mut validator = Validator::new();
    validator.commit(vec![age_col, score_col]).unwrap();

    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    // Expected: 3 errors (150 > 120, -5 < 0, 105 > 100)
    assert_eq!(error_count, 3);
}

#[test]
fn test_validator_float_column_validation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "price").unwrap();
    writeln!(file, "10.5").unwrap(); // ok
    writeln!(file, "25.0").unwrap(); // ok
    writeln!(file, "5.0").unwrap(); // fail (not monotonically increasing)
    writeln!(file, "30.0").unwrap(); // ok

    let price_col = FloatColumnBuilder::new("price".to_string())
        .is_monotonically_increasing()
        .build();

    let mut validator = Validator::new();
    validator.commit(vec![price_col]).unwrap();

    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    // Expected: 1 error (5.0 < 25.0 violates monotonicity)
    assert_eq!(error_count, 1);
}

#[test]
fn test_validator_get_rules() {
    let col1 = StringColumnBuilder::new("col1".to_string())
        .with_length_between(Some(1), Some(10))
        .build();

    let col2 = StringColumnBuilder::new("col2".to_string())
        .with_regex("^[a-z]+$".to_string(), None)
        .unwrap()
        .build();

    let col3 = IntegerColumnBuilder::new("col3".to_string())
        .between(Some(2), Some(5))
        .build();

    let mut validator = Validator::new();
    validator.commit(vec![col1, col2, col3]).unwrap();

    let rules = validator.get_rules();
    assert_eq!(rules.len(), 3);
    assert_eq!(
        rules.get("col1").unwrap(),
        &vec!["TypeCheck".to_string(), "StringLengthCheck".to_string()]
    );
    assert_eq!(
        rules.get("col2").unwrap(),
        &vec!["TypeCheck".to_string(), "RegexMatch".to_string()]
    );
    assert_eq!(
        rules.get("col3").unwrap(),
        &vec!["TypeCheck".to_string(), "NumericRange".to_string()]
    );
}

#[test]
fn test_validator_multiple_rules_per_column() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "username").unwrap();
    writeln!(file, "alice").unwrap(); // ok
    writeln!(file, "ab").unwrap(); // fail (too short)
    writeln!(file, "bob123").unwrap(); // fail (not alpha)
    writeln!(file, "charlie").unwrap(); // ok
    writeln!(file, "verylongusernamethatexceedslimit").unwrap(); // fail (too long)

    let username_col = StringColumnBuilder::new("username".to_string())
        .with_min_length(3)
        .with_max_length(20)
        .is_alpha()
        .unwrap()
        .build();

    let mut validator = Validator::new();
    validator.commit(vec![username_col]).unwrap();

    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    // Expected:
    // - "ab": fail (length < 3) = 1 error
    // - "bob123": fail (not alpha) = 1 error
    // - "verylongusername...": fail (length > 20) = 1 error
    assert_eq!(error_count, 3);
}

#[test]
fn test_validator_all_pass() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "name,age").unwrap();
    writeln!(file, "alice,25").unwrap();
    writeln!(file, "bob,30").unwrap();
    writeln!(file, "charlie,35").unwrap();

    let name_col = StringColumnBuilder::new("name".to_string())
        .with_min_length(3)
        .build();

    let age_col = IntegerColumnBuilder::new("age".to_string())
        .is_positive()
        .build();

    let mut validator = Validator::new();
    validator.commit(vec![name_col, age_col]).unwrap();

    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    assert_eq!(error_count, 0);
}

#[test]
fn test_validator_email_validation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "email").unwrap();
    writeln!(file, "test@example.com").unwrap(); // ok
    writeln!(file, "invalid-email").unwrap(); // fail
    writeln!(file, "another@test.co.uk").unwrap(); // ok
    writeln!(file, "@invalid.com").unwrap(); // fail

    let email_col = StringColumnBuilder::new("email".to_string())
        .is_email()
        .unwrap()
        .build();

    let mut validator = Validator::new();
    validator.commit(vec![email_col]).unwrap();

    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    // Expected: 2 errors (invalid-email, @invalid.com)
    assert_eq!(error_count, 2);
}

#[test]
fn test_validator_mixed_column_types() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "name,age,score,price").unwrap();
    writeln!(file, "alice,25,85,10.5").unwrap(); // all ok
    writeln!(file, "ab,150,95,25.0").unwrap(); // name fail, age fail
    writeln!(file, "charlie,30,105,30.0").unwrap(); // score fail
    writeln!(file, "dave,35,90,5.0").unwrap(); // price fail (monotonicity)

    let name_col = StringColumnBuilder::new("name".to_string())
        .with_min_length(3)
        .build();

    let age_col = IntegerColumnBuilder::new("age".to_string())
        .between(Some(0), Some(120))
        .build();

    let score_col = IntegerColumnBuilder::new("score".to_string())
        .between(Some(0), Some(100))
        .build();

    let price_col = FloatColumnBuilder::new("price".to_string())
        .is_monotonically_increasing()
        .build();

    let mut validator = Validator::new();
    validator
        .commit(vec![name_col, age_col, score_col, price_col])
        .unwrap();

    let error_count = validator
        .validate_csv(file_path.to_str().unwrap(), false)
        .unwrap();

    // Expected: 4 errors (ab too short, 150 > 120, 105 > 100, 5.0 < 30.0)
    assert_eq!(error_count, 4);
}
