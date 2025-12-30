use dataguard_core::{
    columns::date_builder::DateColumnBuilder as CoreDateColumnBuilder, errors::RuleError,
};
use pyo3::prelude::*;

/// Python wrapper for DateColumnBuilder from dataguard-core.
///
/// A builder for defining validation rules on date columns.
#[pyclass(name = "DateColumnBuilder")]
#[derive(Clone)]
pub struct DateColumnBuilder {
    inner: CoreDateColumnBuilder,
}

impl DateColumnBuilder {
    /// Convert this Python builder to a core ColumnBuilder trait object
    pub fn to_core_column_builder(
        &self,
    ) -> Result<Box<dyn dataguard_core::columns::ColumnBuilder>, RuleError> {
        Ok(Box::new(self.inner.clone()))
    }
}

#[pymethods]
impl DateColumnBuilder {
    /// Set the type checking threshold.
    ///
    /// Args:
    ///     threshold (float): The threshold for type checking (0.0 to 1.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    pub fn with_type_threshold(&mut self, threshold: f64) -> Self {
        self.inner = self.inner.clone().with_type_threshold(threshold);
        self.clone()
    }

    /// Get the date format string.
    ///
    /// Returns:
    ///     str: The date format string.
    pub fn get_format(&self) -> String {
        self.inner.get_format()
    }

    /// Add a not-null constraint.
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of null values allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
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
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_unique(&mut self, threshold: f64) -> Self {
        self.inner.is_unique(threshold);
        self.clone()
    }

    /// Set a limit - the date should be before the given date.
    ///
    /// Args:
    ///     year (int): Year value.
    ///     month (int | None): Optional month value.
    ///     day (int | None): Optional day value.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (year, month=None, day=None, threshold=0.0))]
    pub fn is_before(
        &mut self,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
        threshold: f64,
    ) -> Self {
        self.inner.is_before(year, month, day, threshold);
        self.clone()
    }

    /// Set a limit - the date should be after the given date.
    ///
    /// Args:
    ///     year (int): Year value.
    ///     month (int | None): Optional month value.
    ///     day (int | None): Optional day value.
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (year, month=None, day=None, threshold=0.0))]
    pub fn is_after(
        &mut self,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
        threshold: f64,
    ) -> Self {
        self.inner.is_after(year, month, day, threshold);
        self.clone()
    }

    /// Check that all dates are not in the future (before today).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_not_futur(&mut self, threshold: f64) -> Self {
        self.inner.is_not_futur(threshold);
        self.clone()
    }

    /// Check that all dates are not in the past (after today).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_not_past(&mut self, threshold: f64) -> Self {
        self.inner.is_not_past(threshold);
        self.clone()
    }

    /// Check that dates fall on weekdays (Monday-Friday).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_weekday(&mut self, threshold: f64) -> Self {
        self.inner.is_weekday(threshold);
        self.clone()
    }

    /// Check that dates fall on weekends (Saturday-Sunday).
    ///
    /// Args:
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     DateColumnBuilder: Self for method chaining.
    #[pyo3(signature = (threshold=0.0))]
    pub fn is_weekend(&mut self, threshold: f64) -> Self {
        self.inner.is_weekend(threshold);
        self.clone()
    }
}

/// Creates a builder for defining rules on a date column.
///
/// Args:
///     name (str): The name of the column.
///     format (str): The date format string (e.g., "%Y-%m-%d").
///
/// Returns:
///     DateColumnBuilder: A builder object for chaining rules.
#[pyfunction]
pub fn date_column(name: String, format: String) -> DateColumnBuilder {
    DateColumnBuilder {
        inner: CoreDateColumnBuilder::new(name, format),
    }
}
