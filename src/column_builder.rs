use std::{
    sync::{Arc, Mutex},
};

use arrow::datatypes::DataType;
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::{rules::{NotUnique, TypeCheck}, types::RuleMap};

#[pyclass]
pub struct ColumnBuilder {
    pub(crate) column: String,
    rules_map: Arc<Mutex<RuleMap>>,
}

impl ColumnBuilder {
    pub fn new(
        column: String,
        rules_map: Arc<Mutex<RuleMap>>,
    ) -> Self {
        Self { column, rules_map }
    }
}

#[pymethods]
impl ColumnBuilder {
    #[getter]
    fn column_name(&self) -> &str {
        &self.column
    }

    /// Add a type-check rule
    fn type_check<'py>(slf: PyRefMut<'py, Self>, type_name: &str) -> PyResult<PyRefMut<'py, Self>> {
        let data_type = match type_name {
            "string" => DataType::Utf8,
            "int" => DataType::Int64,
            _ => return Err(PyErr::new::<PyValueError, _>("Unknown column type")),
        };
        let rule = TypeCheck::new(slf.column.clone(), data_type);
        {
            let mut mapper = slf.rules_map.lock().unwrap();
            mapper
                .entry(slf.column.clone())
                .or_insert_with(|| Vec::new())
                .push(Box::new(rule));
        }

        Ok(slf)
    }

    /// Add a not-unique rule
    fn not_unique<'py>(slf: PyRefMut<'py, Self>) -> PyResult<PyRefMut<'py, Self>> {
        let rule = NotUnique::new(slf.column.clone());
        {
            let mut mapper = slf.rules_map.lock().unwrap();
            mapper
                .entry(slf.column.clone())
                .or_insert_with(|| Vec::new())
                .push(Box::new(rule));
        }
        Ok(slf)
    }
}
