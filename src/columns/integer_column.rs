use crate::{columns::Column, rules::core::Rule};
use pyo3::prelude::*;

#[pyclass(name = "IntegerColumnBuilder")]
pub struct IntegerColumnBuilder {
    name: String,
    rules: Vec<Rule>,
}

#[pymethods]
impl IntegerColumnBuilder {
    #[new]
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
        }
    }

    /// Add a rule to check that the length of a string is comprised between a min and a max.
    pub fn with_range(&mut self, min: Option<usize>, max: Option<usize>) -> PyResult<Self> {
        self.rules.push(Rule::IntegerRange { min, max });
        Ok(self.clone())
    }

    /// Add a rule to check the minimun length required for a string to be valid.
    pub fn with_min(&mut self, min: usize) -> PyResult<Self> {
        self.rules.push(Rule::IntegerRange {
            min: Some(min),
            max: None,
        });
        Ok(self.clone())
    }

    /// Add a rule to check the maximum length required for a string to be valid.
    pub fn with_max(&mut self, max: usize) -> PyResult<Self> {
        self.rules.push(Rule::IntegerRange {
            min: None,
            max: Some(max),
        });
        Ok(self.clone())
    }

    /// Build the Column object.
    pub fn build(&self) -> Column {
        Column::new(self.name.clone(), "integer".to_string(), self.rules.clone())
    }
}

// pyo3 requires a `clone` implementation for `with_` methods to return `Self`
impl Clone for IntegerColumnBuilder {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            rules: self.rules.clone(),
        }
    }
}
