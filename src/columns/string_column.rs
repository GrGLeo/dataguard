use super::Column;
use crate::{errors::RuleError, rules::core::Rule};
use pyo3::prelude::*;
use regex::Regex;

#[pyclass(name = "StringColumnBuilder")]
pub struct StringColumnBuilder {
    name: String,
    rules: Vec<Rule>,
}

#[pymethods]
impl StringColumnBuilder {
    #[new]
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
        }
    }

    /// Add a rule to check that the length of a string is comprised between a min and a max.
    pub fn with_length_between(
        &mut self,
        min: Option<usize>,
        max: Option<usize>,
    ) -> PyResult<Self> {
        self.rules.push(Rule::StringLength { min, max });
        Ok(self.clone())
    }

    /// Add a rule to check the minimun length required for a string to be valid.
    pub fn with_min_length(&mut self, min: usize) -> PyResult<Self> {
        self.rules.push(Rule::StringLength {
            min: Some(min),
            max: None,
        });
        Ok(self.clone())
    }

    /// Add a rule to check the maximum length required for a string to be valid.
    pub fn with_max_length(&mut self, max: usize) -> PyResult<Self> {
        self.rules.push(Rule::StringLength {
            min: None,
            max: Some(max),
        });
        Ok(self.clone())
    }

    /// Add a rule to match a string against a regex pattern.
    pub fn with_regex(&mut self, pattern: &str, flag: Option<&str>) -> PyResult<Self> {
        let _ = Regex::new(pattern).map_err(|e| {
            RuleError::ValidationError(format!("Invalid regex pattern: '{}': '{}'", pattern, e))
        })?;
        let flag = flag.map(|f| f.to_string());
        self.rules.push(Rule::StringRegex {
            pattern: pattern.to_string(),
            flag,
        });
        Ok(self.clone())
    }

    /// Build the Column object.
    pub fn build(&self) -> Column {
        Column::new(self.name.clone(), "string".to_string(), self.rules.clone())
    }
}

// pyo3 requires a `clone` implementation for `with_` methods to return `Self`
impl Clone for StringColumnBuilder {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            rules: self.rules.clone(),
        }
    }
}
