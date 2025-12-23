use dataguard_core::{
    columns::date_builder::DateColumnBuilder, columns::relation_builder::RelationBuilder,
    utils::operator::CompOperator, CsvTable, NumericColumnBuilder, StringColumnBuilder, Table,
};
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_table_string_column_validation() {
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
    let mut desc_col = StringColumnBuilder::new("description".to_string());
    desc_col
        .with_regex("^[a-z ]+$".to_string(), None)
        .unwrap()
        .with_min_length(6);

    let file_path = file_path.into_os_string().into_string().unwrap();

    // Commit to validator
    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table.prepare(vec![Box::new(desc_col)], vec![]).unwrap();

    // Run validation
    let _res = csv_table.validate();

    // Expected errors:
    // - "short": fail (length < 6) + pass (regex) = 1 error
    // - "a good description": pass + pass = 0 errors
    // - "invalid-char!": pass (length) + fail (regex has !) = 1 error
    // - "another good one": pass + pass = 0 errors
    // - "": fail (length) + fail (regex) = 2 errors
    if let Ok(res) = csv_table.validate() {
        assert!(!res.is_passed());
    } else {
        assert!(false)
    }
}

#[test]
fn test_table_integer_column_validation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "age,score").unwrap();
    writeln!(file, "25,95").unwrap(); // ok
    writeln!(file, "150,85").unwrap(); // age fail (>120)
    writeln!(file, "-5,75").unwrap(); // age fail (<0)
    writeln!(file, "30,105").unwrap(); // score fail (>100)
    writeln!(file, "45,50").unwrap(); // ok

    let mut age_col = NumericColumnBuilder::<i64>::new("age".to_string());
    age_col.between(0, 120);

    let mut score_col = NumericColumnBuilder::<i64>::new("score".to_string());
    score_col.between(0, 100);

    let file_path = file_path.into_os_string().into_string().unwrap();
    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(age_col), Box::new(score_col)], vec![])
        .unwrap();

    let _res = csv_table.validate();

    // Expected: 3 errors (150 > 120, -5 < 0, 105 > 100)
    if let Ok(res) = csv_table.validate() {
        assert!(!res.is_passed());
    } else {
        assert!(false)
    }
}

#[test]
fn test_table_float_column_validation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "price").unwrap();
    writeln!(file, "10.5").unwrap(); // ok
    writeln!(file, "25.0").unwrap(); // ok
    writeln!(file, "5.0").unwrap(); // fail (not monotonically increasing)
    writeln!(file, "30.0").unwrap(); // ok

    let file_path = file_path.into_os_string().into_string().unwrap();

    let mut price_col = NumericColumnBuilder::<f64>::new("price".to_string());
    price_col.is_monotonically_increasing();

    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(price_col)], vec![])
        .unwrap();

    // Expected: 1 error (5.0 < 25.0 violates monotonicity)
    if let Ok(res) = csv_table.validate() {
        assert!(!res.is_passed());
    } else {
        assert!(false)
    }
}

#[test]
fn test_table_get_rules() {
    let mut col1 = StringColumnBuilder::new("col1".to_string());
    col1.with_length_between(1, 10);

    let mut col2 = StringColumnBuilder::new("col2".to_string());
    col2.with_regex("^[a-z]+$".to_string(), None).unwrap();

    let mut col3 = NumericColumnBuilder::<i64>::new("col3".to_string());
    col3.between(2, 5);

    let mut csv_table = CsvTable::new("hi".to_string(), "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(col1), Box::new(col2), Box::new(col3)], vec![])
        .unwrap();

    let rules = csv_table.get_rules();
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
fn test_table_multiple_rules_per_column() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "username, city").unwrap();
    writeln!(file, "alice, paris").unwrap(); // ok
    writeln!(file, "ab, paris").unwrap(); // fail (too short)
    writeln!(file, "bob123, marseille").unwrap(); // fail (not alpha)
    writeln!(file, "charlie, lyon").unwrap(); // ok
    writeln!(file, "verylongusernamethatexceedslimit, grenoble").unwrap(); // fail (too long)

    let file_path = file_path.into_os_string().into_string().unwrap();

    let mut username_col = StringColumnBuilder::new("username".to_string());
    username_col
        .with_min_length(3)
        .with_max_length(20)
        .is_alpha()
        .unwrap();

    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(username_col)], vec![])
        .unwrap();

    // Expected:
    // - "ab": fail (length < 3) = 1 error
    // - "bob123": fail (not alpha) = 1 error
    // - "verylongusername...": fail (length > 20) = 1 error
    if let Ok(res) = csv_table.validate() {
        assert!(!res.is_passed());
    } else {
        assert!(false)
    }
}

#[test]
fn test_table_all_pass() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "name,age").unwrap();
    writeln!(file, "alice,25").unwrap();
    writeln!(file, "bob,30").unwrap();
    writeln!(file, "charlie,35").unwrap();

    let mut name_col = StringColumnBuilder::new("name".to_string());
    name_col.with_min_length(3);

    let mut age_col = NumericColumnBuilder::<i64>::new("age".to_string());
    age_col.is_positive();

    let file_path = file_path.into_os_string().into_string().unwrap();
    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(name_col), Box::new(age_col)], vec![])
        .unwrap();

    let res = csv_table.validate();

    assert_eq!(res.is_ok(), true);
}

#[test]
fn test_table_email_validation() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "email").unwrap();
    writeln!(file, "test@example.com").unwrap(); // ok
    writeln!(file, "invalid-email").unwrap(); // fail
    writeln!(file, "another@test.co.uk").unwrap(); // ok
    writeln!(file, "@invalid.com").unwrap(); // fail

    let mut email_col = StringColumnBuilder::new("email".to_string());
    email_col.is_email().unwrap();

    let file_path = file_path.into_os_string().into_string().unwrap();
    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table
        .prepare(vec![Box::new(email_col)], vec![])
        .unwrap();

    if let Ok(res) = csv_table.validate() {
        assert!(!res.is_passed());
    } else {
        assert!(false)
    }
}

#[test]
fn test_table_mixed_column_types() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();
    writeln!(file, "name,age,score,price,start_date,end_date").unwrap();
    writeln!(file, "alice,25,85,10.5,2024-01-01,2024-12-31").unwrap(); // all ok
    writeln!(file, "ab,150,95,25.0,2024-06-01,2024-03-01").unwrap(); // name fail, age fail, date relation fail (end < start)
    writeln!(file, "charlie,30,105,30.0,2024-01-15,2024-01-20").unwrap(); // score fail
    writeln!(file, "dave,35,90,5.0,2024-02-01,2024-02-28").unwrap(); // price fail (monotonicity)

    let mut name_col = StringColumnBuilder::new("name".to_string());
    name_col.with_min_length(3);

    let mut age_col = NumericColumnBuilder::<i64>::new("age".to_string());
    age_col.between(0, 120);

    let mut score_col = NumericColumnBuilder::<i64>::new("score".to_string());
    score_col.between(0, 100);

    let mut price_col = NumericColumnBuilder::<f64>::new("price".to_string());
    price_col.is_monotonically_increasing();

    let email_col = StringColumnBuilder::new("email".to_string());

    let start_date_col = DateColumnBuilder::new("start_date".to_string(), "%Y-%m-%d".to_string());
    let end_date_col = DateColumnBuilder::new("end_date".to_string(), "%Y-%m-%d".to_string());

    // Create relation: start_date <= end_date
    let mut date_relation =
        RelationBuilder::new(["start_date".to_string(), "end_date".to_string()]);
    date_relation.date_comparaison(CompOperator::Lte);

    let file_path = file_path.into_os_string().into_string().unwrap();
    let mut csv_table = CsvTable::new(file_path, "stdout".to_string()).unwrap();
    csv_table
        .prepare(
            vec![
                Box::new(name_col),
                Box::new(age_col),
                Box::new(score_col),
                Box::new(price_col),
                Box::new(email_col),
                Box::new(start_date_col),
                Box::new(end_date_col),
            ],
            vec![date_relation],
        )
        .unwrap();

    // Expected: 4 column errors (ab too short, 150 > 120, 105 > 100, 5.0 < 30.0)
    // Plus 1 relation error (2024-06-01 > 2024-03-01 violates start_date <= end_date)
    if let Ok(res) = csv_table.validate() {
        assert!(!res.is_passed());
    } else {
        assert!(false)
    }
}
