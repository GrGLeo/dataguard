pub mod columns;
pub mod errors;
pub mod reader;
pub mod report;
pub mod rules;
pub mod types;
pub mod utils;

pub mod validator;

#[cfg(feature = "python")]
use crate::columns::float_column::FloatColumnBuilder;
#[cfg(feature = "python")]
use crate::columns::integer_column::IntegerColumnBuilder;
use crate::columns::{Column, string_column::StringColumnBuilder};
use pyo3::prelude::*;

/// Creates a builder for defining rules on a string column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     StringColumnBuilder: A builder object for chaining rules.
#[cfg(feature = "python")]
#[pyfunction]
fn string_column(name: String) -> PyResult<StringColumnBuilder> {
    Ok(StringColumnBuilder::new(name))
}

/// Creates a builder for defining rules on a integer column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     IntegerColumnBuilder: A builder object for chaining rules.
#[cfg(feature = "python")]
#[pyfunction]
fn integer_column(name: String) -> PyResult<IntegerColumnBuilder> {
    Ok(IntegerColumnBuilder::new(name))
}

/// Creates a builder for defining rules on a float column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     FloatColumnBuilder: A builder object for chaining rules.
#[cfg(feature = "python")]
#[pyfunction]
fn float_column(name: String) -> PyResult<FloatColumnBuilder> {
    Ok(FloatColumnBuilder::new(name))
}

/// DataGuard: A high-performance CSV validation library.
#[cfg(feature = "python")]
#[pyo3::pymodule]
fn dataguard(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<validator::Validator>()?;
    m.add_class::<Column>()?;
    m.add_class::<StringColumnBuilder>()?;
    m.add_class::<IntegerColumnBuilder>()?;
    m.add_class::<FloatColumnBuilder>()?;
    m.add_function(wrap_pyfunction!(string_column, m)?)?;
    m.add_function(wrap_pyfunction!(integer_column, m)?)?;
    m.add_function(wrap_pyfunction!(float_column, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validator::Validator;
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_builder_commit_and_get_rules() {
        // 1. Create builders and build Column DTOs
        let col1 = string_column("col1".to_string())
            .unwrap()
            .with_length_between(Some(1), Some(10)) // Changed
            .unwrap()
            .build();

        let col2 = string_column("col2".to_string())
            .unwrap()
            .with_regex("^[a-z]+$", None) // Changed
            .unwrap()
            .build();

        let col3 = integer_column("col3".to_string())
            .unwrap()
            .between(Some(2i64), Some(5i64))
            .unwrap()
            .build();

        // 2. Create a validator and commit the columns
        let mut validator = Validator::new();
        validator.commit(vec![col1, col2, col3]).unwrap();

        // 3. Check the internal state via get_rules()
        let rules = validator.get_rules().unwrap();
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
    fn test_validate_csv_end_to_end() {
        // 1. Setup: Create a temporary CSV file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "product_id,description,price").unwrap();
        writeln!(file, "p1,short,10.0").unwrap(); // desc length fail (<6)
        writeln!(file, "p2,a good description,20.0").unwrap(); // ok
        writeln!(file, "p3,invalid-char!,30.0").unwrap(); // desc regex fail (! is invalid)
        writeln!(file, "p4,another good one,40.0").unwrap(); // ok
        writeln!(file, "p5,,50.0").unwrap(); // desc length fail ("" < 6)
        writeln!(file, "p6,12345,60.0").unwrap(); // desc regex fail (numbers not allowed)

        // 2. Create column rules
        let desc_col = string_column("description".to_string())
            .unwrap()
            .with_regex("^[a-z ]+$", None) // Changed
            .unwrap()
            .with_min_length(6) // Changed
            .unwrap()
            .build();

        // 3. Commit to validator
        let mut validator = Validator::new();
        validator.commit(vec![desc_col]).unwrap();

        // 4. Run validation
        let error_count = validator
            .validate_csv(file_path.to_str().unwrap(), false)
            .unwrap();

        // 5. Assert results
        // Expected errors:
        // - "short": pass | fail
        // - "a good description": pass | pass
        // - "invalid-char!": fail | pass
        // - "": fail | fail
        // - "12345": fail | fail
        assert_eq!(error_count, 6);

        // The tempdir will be automatically cleaned up when `dir` goes out of scope.
    }

    #[test]
    fn test_float_column_between() {
        let col = float_column("col1".to_string())
            .unwrap()
            .between(Some(1.0), Some(5.0))
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "NumericRange".to_string()]
        );
    }

    #[test]
    fn test_float_column_is_positive() {
        let col = float_column("col1".to_string())
            .unwrap()
            .is_positive()
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "NumericRange".to_string()]
        );
    }

    #[test]
    fn test_float_column_is_negative() {
        let col = float_column("col1".to_string())
            .unwrap()
            .is_negative()
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "NumericRange".to_string()]
        );
    }

    #[test]
    fn test_float_column_is_non_positive() {
        let col = float_column("col1".to_string())
            .unwrap()
            .is_non_positive()
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "NumericRange".to_string()]
        );
    }

    #[test]
    fn test_float_column_is_non_negative() {
        let col = float_column("col1".to_string())
            .unwrap()
            .is_non_negative()
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "NumericRange".to_string()]
        );
    }

    #[test]
    fn test_float_monotonicity_asc_valid() {
        let col = float_column("col1".to_string())
            .unwrap()
            .is_monotonically_increasing()
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "Monotonicity".to_string()]
        );

        // Test with valid data
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("asc_valid.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "col1").unwrap();
        writeln!(file, "1.0").unwrap();
        writeln!(file, "2.0").unwrap();
        writeln!(file, "2.0").unwrap();
        writeln!(file, "3.0").unwrap();

        let error_count = validator
            .validate_csv(file_path.to_str().unwrap(), false)
            .unwrap();
        assert_eq!(error_count, 0);

        // Test with invalid data
        let file_path_invalid = dir.path().join("asc_invalid.csv");
        let mut file_invalid = File::create(&file_path_invalid).unwrap();
        writeln!(file_invalid, "col1").unwrap();
        writeln!(file_invalid, "1.0").unwrap();
        writeln!(file_invalid, "3.0").unwrap();
        writeln!(file_invalid, "2.0").unwrap(); // <-- invalid
        writeln!(file_invalid, "4.0").unwrap();

        let col_invalid = float_column("col1".to_string())
            .unwrap()
            .is_monotonically_increasing()
            .unwrap()
            .build();
        let mut validator_invalid = Validator::new();
        validator_invalid.commit(vec![col_invalid]).unwrap();
        let error_count_invalid = validator_invalid
            .validate_csv(file_path_invalid.to_str().unwrap(), false)
            .unwrap();
        assert_eq!(error_count_invalid, 1);
    }

    #[test]
    fn test_float_monotonicity_desc_valid() {
        let col = float_column("col1".to_string())
            .unwrap()
            .is_monotonically_decreasing()
            .unwrap()
            .build();
        let mut validator = Validator::new();
        validator.commit(vec![col]).unwrap();

        let rules = validator.get_rules().unwrap();
        assert_eq!(
            rules.get("col1").unwrap(),
            &vec!["TypeCheck".to_string(), "Monotonicity".to_string()]
        );

        // Test with valid data
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("desc_valid.csv");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "col1").unwrap();
        writeln!(file, "3.0").unwrap();
        writeln!(file, "2.0").unwrap();
        writeln!(file, "2.0").unwrap();
        writeln!(file, "1.0").unwrap();

        let error_count = validator
            .validate_csv(file_path.to_str().unwrap(), false)
            .unwrap();
        assert_eq!(error_count, 0);

        // Test with invalid data
        let file_path_invalid = dir.path().join("desc_invalid.csv");
        let mut file_invalid = File::create(&file_path_invalid).unwrap();
        writeln!(file_invalid, "col1").unwrap();
        writeln!(file_invalid, "4.0").unwrap();
        writeln!(file_invalid, "2.0").unwrap();
        writeln!(file_invalid, "3.0").unwrap(); // <-- invalid
        writeln!(file_invalid, "1.0").unwrap();

        let col_invalid = float_column("col1".to_string())
            .unwrap()
            .is_monotonically_decreasing()
            .unwrap()
            .build();
        let mut validator_invalid = Validator::new();
        validator_invalid.commit(vec![col_invalid]).unwrap();
        let error_count_invalid = validator_invalid
            .validate_csv(file_path_invalid.to_str().unwrap(), false)
            .unwrap();
        assert_eq!(error_count_invalid, 1);
    }
}
