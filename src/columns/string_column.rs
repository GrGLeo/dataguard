use super::Column;
use crate::{errors::RuleError, rules::core::Rule};
use pyo3::prelude::*;
use regex::Regex;

#[pyclass(name = "StringColumnBuilder")]
pub struct StringColumnBuilder {
    name: String,
    rules: Vec<Rule>,
    unicity: Option<Rule>,
}

#[pymethods]
impl StringColumnBuilder {
    #[new]
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
            unicity: None,
        }
    }

    /// Add a rule to check that value are unique. Count the row with duplicates.
    pub fn is_unique(&mut self) -> PyResult<Self> {
        self.unicity = Some(Rule::Unicity {});
        Ok(self.clone())
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

    /// Add a rule to check that the length match exactly.
    pub fn is_exact_length(&mut self, len: usize) -> PyResult<Self> {
        self.rules.push(Rule::StringLength {
            min: Some(len),
            max: Some(len),
        });
        Ok(self.clone())
    }

    pub fn is_in(&mut self, members: Vec<String>) -> PyResult<Self> {
        self.rules.push(Rule::StringMembers { members });
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

    /// Add a rule to check if a string contains only numeric characters.
    pub fn is_numeric(&mut self) -> PyResult<Self> {
        self.with_regex(r"^\d+$", None)
    }

    /// Add a rule to check if a string contains only alphabetic characters.
    pub fn is_alpha(&mut self) -> PyResult<Self> {
        self.with_regex(r"^[a-zA-Z]+$", None)
    }

    /// Add a rule to check if a string contains only alphanumeric characters.
    pub fn is_alphanumeric(&mut self) -> PyResult<Self> {
        self.with_regex(r"^[a-zA-Z0-9]+$", None)
    }

    /// Add a rule to check if a string is lowercase.
    /// This rule checks that there are no uppercase letters in the string.
    /// Non-alphabetic characters are ignored.
    pub fn is_lowercase(&mut self) -> PyResult<Self> {
        self.with_regex(r"^[^A-Z]*$", None)
    }

    /// Add a rule to check if a string is uppercase.
    /// This rule checks that there are no lowercase letters in the string.
    /// Non-alphabetic characters are ignored.
    pub fn is_uppercase(&mut self) -> PyResult<Self> {
        self.with_regex(r"^[^a-z]*$", None)
    }

    /// Add a rule to check if a string is a valid URL.
    pub fn is_url(&mut self) -> PyResult<Self> {
        self.with_regex(r"^(https?|ftp)://[^\s/$.?#].[^\s]*$", None)
    }

    /// Add a rule to check if a string is a valid email address (simple check).
    pub fn is_email(&mut self) -> PyResult<Self> {
        self.with_regex(
            r"^[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$",
            None,
        )
    }

    /// Add a rule to check if a string is a valid UUID.
    pub fn is_uuid(&mut self) -> PyResult<Self> {
        self.with_regex(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            None,
        )
    }

    /// Build the Column object.
    pub fn build(&self) -> Column {
        Column::new(
            self.name.clone(),
            "string".to_string(),
            self.rules.clone(),
            self.unicity.clone(),
        )
    }
}

// pyo3 requires a `clone` implementation for `with_` methods to return `Self`
impl Clone for StringColumnBuilder {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            rules: self.rules.clone(),
            unicity: self.unicity.clone(),
        }
    }
}
