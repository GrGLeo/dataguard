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

    /// Add a rule to check that the value is comprised between a min and a max.
    pub fn between(&mut self, min: Option<i64>, max: Option<i64>) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: min.map(|v| v as f64),
            max: max.map(|v| v as f64),
        });
        Ok(self.clone())
    }

    /// Add a rule to check the minimun value required to be valid.
    pub fn min(&mut self, min: i64) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: Some(min as f64),
            max: None,
        });
        Ok(self.clone())
    }

    /// Add a rule to check the maximum value required to be valid.
    pub fn max(&mut self, max: i64) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: None,
            max: Some(max as f64),
        });
        Ok(self.clone())
    }

    /// Add a rule to check that all values are strictly positive (> 0).
    pub fn is_positive(&mut self) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: Some(1.0),
            max: None,
        });
        Ok(self.clone())
    }

    /// Add a rule to check that all values are strictly negative (< 0).
    pub fn is_negative(&mut self) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: None,
            max: Some(-1.0),
        });
        Ok(self.clone())
    }

    /// Add a rule to check that all values are non-positive (<= 0).
    pub fn is_non_positive(&mut self) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: None,
            max: Some(0.0),
        });
        Ok(self.clone())
    }

    /// Add a rule to check that all values are non-negative (>= 0).
    pub fn is_non_negative(&mut self) -> PyResult<Self> {
        self.rules.push(Rule::NumericRange {
            min: Some(0.0),
            max: None,
        });
        Ok(self.clone())
    }

    /// Add a rule to check monoticity it is valide if  A[i] >= A[i-1]
    pub fn is_monotonically_increasing(&mut self) -> PyResult<Self> {
        self.rules.push(Rule::Monotonicity { asc: true });
        Ok(self.clone())
    }

    /// Add a rule to check monoticity it is valide if  A[i] <= A[i-1]
    pub fn is_monotonically_decreasing(&mut self) -> PyResult<Self> {
        self.rules.push(Rule::Monotonicity { asc: false });
        Ok(self.clone())
    }

    /// Build the Column object.
    pub fn build(&self) -> Column {
        Column::new(
            self.name.clone(),
            "integer".to_string(),
            self.rules.clone(),
            None,
        )
    }
}

impl Clone for IntegerColumnBuilder {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            rules: self.rules.clone(),
        }
    }
}
