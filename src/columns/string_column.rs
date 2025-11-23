use arrow_string::length::length;
use pyo3::{exceptions::PyValueError, prelude::*};
use regex::Regex;

use crate::{columns::{Column, ColumnBuilder}, errors::RuleError, rules::{Operator, RegexMatch, Rule, StringLengthCheck}};


#[cfg(feature = "python")]
#[pyclass]
pub struct StringColumnBuilder {
    column_name: String,
    min_length: Option<u32>,
    max_length: Option<u32>,
    regex_pattern: Option<String>,
    regex_flag: Option<String>,
}

#[cfg(feature = "python")]
impl StringColumnBuilder {
    pub fn new(column_name: &str) -> Self {
        Self {
            column_name: column_name.to_string(),
            min_length: None,
            max_length: None,
            regex_pattern: None,
            regex_flag: None,
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl StringColumnBuilder {
    fn with_min_length<'py>(mut slf: PyRefMut<'py, Self>, min: u32) -> PyResult<PyRefMut<'py, Self>> {
        slf.min_length = Some(min);
        Ok(slf)
    }

    fn with_max_length<'py>(mut slf: PyRefMut<'py, Self>, max: u32) -> PyResult<PyRefMut<'py, Self>> {
        slf.max_length = Some(max);
        Ok(slf)
    }

    fn with_length_between<'py>(mut slf: PyRefMut<'py, Self>,min:u32, max: u32) -> PyResult<PyRefMut<'py, Self>> {
        slf.min_length = Some(min);
        slf.max_length = Some(max);
        Ok(slf)
    }

    fn with_regex<'py>(mut slf: PyRefMut<'py, Self>,pattern: &str, flag: Option<&str>) -> PyResult<PyRefMut<'py, Self>> {
        let _ = Regex::new(pattern).map_err(|e| {
            RuleError::ValidationError(format!("Invalid regex: '{}' : '{}'", pattern, e))
        })?;

        let flag = flag.map(|f| f.to_string());

        slf.regex_pattern = Some(pattern.to_string());
        slf.regex_flag = flag;
        Ok(slf)
    }
}

impl ColumnBuilder for StringColumnBuilder {
    fn build(self) -> Column {
        let mut rules: Vec<Box<dyn Rule>> = Vec::new();
        if let Some(min) = self.min_length {
            rules.push(Box::new(StringLengthCheck::new(self.column_name.clone(), min as usize, Operator::Lt)));
        }
        if let Some(max) = self.max_length {
            rules.push(Box::new(StringLengthCheck::new(self.column_name.clone(), max as usize, Operator::Gt)));
        }
        if let Some(pattern) = self.regex_pattern {
            rules.push(Box::new(RegexMatch::new(self.column_name.clone(), pattern, self.regex_flag)));
        }
        Column::new(self.column_name, rules)
    }
}
