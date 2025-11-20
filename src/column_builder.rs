use arrow::datatypes::DataType;
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::rules::{Rule, TypeCheck};

#[pyclass]
pub struct ColumnBuilder {
    column: String,
    rules_vec: Vec<Box<dyn Rule + Send + Sync>>,
}

impl ColumnBuilder {
    pub fn new(column: String) -> Self {
        Self {
            column,
            rules_vec: Vec::new(),
        }
    }
}

#[pymethods]
impl ColumnBuilder {
    /// Add a type-check rule
    fn type_check<'py>(
        mut slf: PyRefMut<'py, Self>,
        type_name: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let data_type = match type_name {
            "string" => DataType::Utf8,
            "int" => DataType::Int64,
            _ => return Err(PyErr::new::<PyValueError, _>("Unknown column type")),
        };
        let rule = TypeCheck::new(slf.column.clone(), data_type);
        slf.rules_vec.push(Box::new(rule));
        Ok(slf)
    }

    /// Add a not-unique rule
    fn not_unique<'py>(mut _slf: PyRefMut<'py, Self>) -> PyResult<PyRefMut<'py, Self>> {
        todo!()
    }

    /// Optional: list rules added for this column
    fn list_rules(&self) -> PyResult<Vec<String>> {
        todo!()
    }
}
