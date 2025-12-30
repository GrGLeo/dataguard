use dataguard_core::{errors::RuleError, NumericColumnBuilder as CoreNumericColumnBuilder};
use pyo3::prelude::*;

/// Python wrapper for FloatColumnBuilder from dataguard-core.
///
/// A builder for defining validation rules on float (f64) columns.
#[pyclass(name = "FloatColumnBuilder")]
#[derive(Clone)]
pub struct FloatColumnBuilder {
    inner: CoreNumericColumnBuilder<f64>,
}

impl FloatColumnBuilder {
    /// Convert this Python builder to a core ColumnBuilder trait object
    pub fn to_core_column_builder(
        &self,
    ) -> Result<Box<dyn dataguard_core::columns::ColumnBuilder>, RuleError> {
        Ok(Box::new(self.inner.clone()))
    }
}

#[pymethods]
impl FloatColumnBuilder {
    /// Set the type checking threshold.
    ///
    /// Args:
    ///     threshold (float): The threshold for type checking (0.0 to 1.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
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
    ///     FloatColumnBuilder: Self for method chaining.
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
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_unique(&mut self, threshold: f64) -> Self {
        self.inner.is_unique(threshold);
        self.clone()
    }

    /// Set numeric range (both min and max).
    ///
    /// Args:
    ///     min (float): Minimum value.
    ///     max (float): Maximum value.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (min, max, threshold=0.0))]
    pub fn between(&mut self, min: f64, max: f64, threshold: f64) -> Self {
        self.inner.between(min, max, threshold);
        self.clone()
    }

    /// Set minimum value.
    ///
    /// Args:
    ///     min (float): Minimum value.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (min, threshold=0.0))]
    pub fn min(&mut self, min: f64, threshold: f64) -> Self {
        self.inner.min(min, threshold);
        self.clone()
    }

    /// Set maximum value.
    ///
    /// Args:
    ///     max (float): Maximum value.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (max, threshold=0.0))]
    pub fn max(&mut self, max: f64, threshold: f64) -> Self {
        self.inner.max(max, threshold);
        self.clone()
    }

    /// Check if values are positive (> 0).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_positive(&mut self, threshold: f64) -> Self {
        self.inner.is_positive(threshold);
        self.clone()
    }

    /// Check if values are negative (< 0).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_negative(&mut self, threshold: f64) -> Self {
        self.inner.is_negative(threshold);
        self.clone()
    }

    /// Check if values are non-negative (>= 0).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_non_negative(&mut self, threshold: f64) -> Self {
        self.inner.is_non_negative(threshold);
        self.clone()
    }

    /// Check if values are non-positive (<= 0).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_non_positive(&mut self, threshold: f64) -> Self {
        self.inner.is_non_positive(threshold);
        self.clone()
    }

    /// Check if values are monotonically increasing.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_monotonically_increasing(&mut self, threshold: f64) -> Self {
        self.inner.is_monotonically_increasing(threshold);
        self.clone()
    }

    /// Check if values are monotonically decreasing.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_monotonically_decreasing(&mut self, threshold: f64) -> Self {
        self.inner.is_monotonically_decreasing(threshold);
        self.clone()
    }

    /// Check if values are within N standard deviations from the mean.
    ///
    /// Args:
    ///     max_std_dev (float): Maximum number of standard deviations allowed.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (max_std_dev, threshold=0.0))]
    pub fn std_dev_check(&mut self, max_std_dev: f64, threshold: f64) -> Self {
        self.inner.std_dev_check(threshold, max_std_dev);
        self.clone()
    }

    /// Check if values deviate from mean by more than a percentage.
    ///
    /// Args:
    ///     max_variance_percent (float): Maximum percentage deviation from mean allowed.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     FloatColumnBuilder: Self for method chaining.
    #[pyo3(signature = (max_variance_percent, threshold=0.0))]
    pub fn mean_variance(&mut self, max_variance_percent: f64, threshold: f64) -> Self {
        self.inner.mean_variance(threshold, max_variance_percent);
        self.clone()
    }
}

/// Creates a builder for defining rules on a float column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     FloatColumnBuilder: A builder object for chaining rules.
#[pyfunction]
pub fn float_column(name: String) -> FloatColumnBuilder {
    FloatColumnBuilder {
        inner: CoreNumericColumnBuilder::<f64>::new(name),
    }
}
