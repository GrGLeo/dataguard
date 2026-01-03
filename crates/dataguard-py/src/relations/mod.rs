use dataguard_core::{
    columns::relation_builder::RelationBuilder as CoreRelationBuilder,
    utils::operator::CompOperator,
};
use pyo3::prelude::*;

/// Python wrapper for RelationBuilder from dataguard-core.
///
/// A builder for defining validation rules between two columns.
#[pyclass(name = "RelationBuilder")]
pub struct RelationBuilder {
    inner: CoreRelationBuilder,
}

impl Clone for RelationBuilder {
    fn clone(&self) -> Self {
        Self {
            inner: CoreRelationBuilder {
                names: self.inner.names(),
                rules: self.inner.rules().to_vec(),
            },
        }
    }
}

impl RelationBuilder {
    /// Convert this Python builder to a core RelationBuilder
    pub fn to_core_relation_builder(&self) -> CoreRelationBuilder {
        CoreRelationBuilder {
            names: self.inner.names(),
            rules: self.inner.rules().to_vec(),
        }
    }
}

#[pymethods]
impl RelationBuilder {
    /// Add a date comparison rule between two date columns.
    ///
    /// Args:
    ///     operator (str): Comparison operator. Can be:
    ///         - "gt" or ">": Greater than
    ///         - "gte" or ">=": Greater than or equal
    ///         - "eq" or "=": Equal
    ///         - "lt" or "<": Less than
    ///         - "lte" or "<=": Less than or equal
    ///     threshold (float): Maximum percentage of violations allowed (default: 0.0).
    ///
    /// Returns:
    ///     RelationBuilder: Self for method chaining.
    #[pyo3(signature = (operator, threshold=0.0))]
    pub fn date_comparaison(&mut self, operator: &str, threshold: f64) -> PyResult<Self> {
        let op = CompOperator::try_from(operator)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        self.inner.date_comparaison(op, threshold);
        Ok(self.clone())
    }

    #[pyo3(signature = (operator, threshold=0.0))]
    pub fn numeric_comparaison(&mut self, operator: &str, threshold: f64) -> PyResult<Self> {
        let op = CompOperator::try_from(operator)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        self.inner.numeric_comparaison(op, threshold);
        Ok(self.clone())
    }
}

/// Creates a builder for defining validation rules between two columns.
///
/// Args:
///     col1 (str): Name of the first column.
///     col2 (str): Name of the second column.
///
/// Returns:
///     RelationBuilder: A builder object for chaining relation rules.
///
/// Example:
///     >>> from dataguard import relation
///     >>> rel = relation("start_date", "end_date").date_comparaison("<")
#[pyfunction]
pub fn relation(col1: String, col2: String) -> RelationBuilder {
    RelationBuilder {
        inner: CoreRelationBuilder::new([col1, col2]),
    }
}
