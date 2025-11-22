use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
#[cfg(feature = "python")]
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::{
    rules::{RegexMatch, TypeCheck, StringLengthCheck},
    types::RuleMap,
};

/// Builder for adding validation rules to a specific CSV column.
#[cfg(feature = "python")]
#[pyclass]
pub struct ColumnBuilder {
    pub(crate) column: String,
    rules_map: Arc<Mutex<RuleMap>>,
}

#[cfg(feature = "python")]
impl ColumnBuilder {
    pub fn new(column: String, rules_map: Arc<Mutex<RuleMap>>) -> Self {
        Self { column, rules_map }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl ColumnBuilder {
    /// Get the name of the column this builder is for.
    ///
    /// Returns:
    ///     str: The column name.
    #[getter]
    fn column_name(&self) -> &str {
        &self.column
    }

    /// Add a type check validation rule for the column.
    ///
    /// Args:
    ///     type_name (str): The expected data type ("string" or "int").
    ///
    /// Returns:
    ///     ColumnBuilder: Self for method chaining.
    fn type_check<'py>(slf: PyRefMut<'py, Self>, type_name: &str) -> PyResult<PyRefMut<'py, Self>> {
        let data_type = match type_name {
            "string" => DataType::Utf8,
            "int" => DataType::Int64,
            _ => return Err(PyErr::new::<PyValueError, _>("Unknown column type")),
        };
        let rule = TypeCheck::new(slf.column.clone(), data_type);
        {
            let mut mapper = slf.rules_map.lock().unwrap();
            mapper
                .entry(slf.column.clone())
                .or_default()
                .push(Box::new(rule));
        }

        Ok(slf)
    }



    /// Add a regex pattern matching validation rule for the column.
    ///
    /// Args:
    ///     pattern (str): The regular expression pattern to match against.
    ///     flag (str, optional): Regex flags (e.g., "i" for case-insensitive).
    ///
    /// Returns:
    ///     ColumnBuilder: Self for method chaining.
    fn regex_match<'py>(
        slf: PyRefMut<'py, Self>,
        pattern: &str,
        flag: Option<&str>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let rule = RegexMatch::new(slf.column.clone(), pattern, flag)?;
        {
            let mut mapper = slf.rules_map.lock().unwrap();
            mapper
                .entry(slf.column.clone())
                .or_default()
                .push(Box::new(rule));
        }
        Ok(slf)
    }

    /// Add a string length check validation rule for the column.
    ///
    /// Args:
    ///     length (int): The length to compare against.
    ///     operator (str): The comparison operator ("lt", "le", "gt", "ge").
    ///
    /// Returns:
    ///     ColumnBuilder: Self for method chaining.
    fn string_length_check<'py>(
        slf: PyRefMut<'py, Self>,
        length: usize,
        operator: &str
    ) -> PyResult<PyRefMut<'py, Self>> {
        let rule = StringLengthCheck::new(slf.column.clone(), length, operator)?;
        {
            let mut mapper = slf.rules_map.lock().unwrap();
            mapper
                .entry(slf.column.clone())
                .or_default()
                .push(Box::new(rule));
        }
        Ok(slf)
    }
}
