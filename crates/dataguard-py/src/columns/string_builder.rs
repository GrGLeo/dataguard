use dataguard_core::{errors::RuleError, StringColumnBuilder as CoreStringColumnBuilder};
use pyo3::prelude::*;

/// Python wrapper for StringColumnBuilder from dataguard-core.
///
/// A builder for defining validation rules on string columns.
#[pyclass(name = "StringColumnBuilder")]
#[derive(Clone)]
pub struct StringColumnBuilder {
    inner: CoreStringColumnBuilder,
}

impl StringColumnBuilder {
    /// Convert this Python builder to a core ColumnBuilder trait object
    pub fn to_core_column_builder(
        &self,
    ) -> Result<Box<dyn dataguard_core::columns::ColumnBuilder>, RuleError> {
        Ok(Box::new(self.inner.clone()))
    }
}

#[pymethods]
impl StringColumnBuilder {
    /// Set the type checking threshold.
    ///
    /// Args:
    ///     threshold (float): The threshold for type checking (0.0 to 1.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    pub fn with_type_threshold(&mut self, threshold: f64) -> Self {
        self.inner = self.inner.clone().with_type_threshold(threshold);
        self.clone()
    }

    /// Add a not-null constraint.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of null values allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_not_null(&mut self, threshold: f64) -> Self {
        self.inner.is_not_null(threshold);
        self.clone()
    }

    /// Add uniqueness constraint.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of duplicate values allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_unique(&mut self, threshold: f64) -> Self {
        self.inner.is_unique(threshold);
        self.clone()
    }

    /// Set length constraints (both min and max).
    ///
    /// Args:
    ///     min (int): Minimum length.
    ///     max (int): Maximum length.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (min, max, threshold=0.0))]
    pub fn with_length_between(&mut self, min: usize, max: usize, threshold: f64) -> Self {
        self.inner.with_length_between(min, max, threshold);
        self.clone()
    }

    /// Set minimum length.
    ///
    /// Args:
    ///     min (int): Minimum length.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (min, threshold=0.0))]
    pub fn with_min_length(&mut self, min: usize, threshold: f64) -> Self {
        self.inner.with_min_length(min, threshold);
        self.clone()
    }

    /// Set maximum length.
    ///
    /// Args:
    ///     max (int): Maximum length.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (max, threshold=0.0))]
    pub fn with_max_length(&mut self, max: usize, threshold: f64) -> Self {
        self.inner.with_max_length(max, threshold);
        self.clone()
    }

    /// Set exact length.
    ///
    /// Args:
    ///     len (int): Exact length required.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (len, threshold=0.0))]
    pub fn is_exact_length(&mut self, len: usize, threshold: f64) -> Self {
        self.inner.is_exact_length(len, threshold);
        self.clone()
    }

    /// Check if value is in a set of allowed values.
    ///
    /// Args:
    ///     members (list[str]): List of allowed values.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (members, threshold=0.0))]
    pub fn is_in(&mut self, members: Vec<String>, threshold: f64) -> Self {
        self.inner.is_in(members, threshold);
        self.clone()
    }

    /// Match against a regex pattern.
    ///
    /// Args:
    ///     pattern (str): Regular expression pattern.
    ///     flags (str | None): Optional regex flags (default: None).
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (pattern, flags=None, threshold=0.0))]
    pub fn with_regex(
        &mut self,
        pattern: String,
        flags: Option<String>,
        threshold: f64,
    ) -> PyResult<Self> {
        self.inner
            .with_regex(pattern, flags, threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string contains only numeric characters.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_numeric(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_numeric(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string contains only alphabetic characters.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_alpha(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_alpha(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string contains only alphanumeric characters.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_alphanumeric(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_alphanumeric(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string is lowercase.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_lowercase(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_lowercase(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string is uppercase.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_uppercase(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_uppercase(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string is a valid URL.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_url(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_url(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string is a valid email.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_email(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_email(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }

    /// Check if string is a valid UUID.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     StringColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_uuid(&mut self, threshold: f64) -> PyResult<Self> {
        self.inner
            .is_uuid(threshold)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(self.clone())
    }
}

/// Creates a builder for defining rules on a string column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     StringColumnBuilder: A builder object for chaining rules.
#[pyfunction]
pub fn string_column(name: String) -> StringColumnBuilder {
    StringColumnBuilder {
        inner: CoreStringColumnBuilder::new(name),
    }
}
