pub mod errors;
pub mod reader;
pub mod column_builder;
pub mod rules;
use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::DataType};
use pyo3::{
    exceptions::{PyIOError, PyValueError},
    prelude::*,
};

use crate::{column_builder::ColumnBuilder, reader::read_csv};

#[pyclass]
struct Validator {
    columns: Vec<(String, DataType)>,
    batches: Vec<Arc<RecordBatch>>,
}

#[pymethods]
impl Validator {
    #[new]
    fn new() -> Self {
        Self {
            columns: Vec::new(),
            batches: Vec::new(),
        }
    }

    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.iter().map(|(c, _)| c.clone()).collect()
    }

    fn add_type_column(&mut self, name: String, column_type: String) -> PyResult<()> {
        match column_type.as_str() {
            "string" => {
                self.columns.push((name, DataType::Utf8));
                Ok(())
            }
            "int" => {
                self.columns.push((name, DataType::Int64));
                Ok(())
            }
            _ => Err(PyErr::new::<PyValueError, _>("Unknown column type")),
        }
    }
    fn add_column_rule(&self, column_name: &str) -> PyResult<ColumnBuilder> {
       Ok(ColumnBuilder::new(column_name.to_string()))
    }

    fn validate_csv(&mut self, path: &str) -> PyResult<usize> {
        if let Ok(batches) = read_csv(path) {
            self.batches = batches;
            Ok(self.batches.iter().map(|b| b.num_rows()).sum())
        } else {
            Err(PyErr::new::<PyIOError, _>("Failed to load CSV"))
        }
    }
}

/// A Python module implemented in Rust.
#[pyo3::pymodule]
mod dataguard {
    use pyo3::prelude::*;

    #[pymodule_export]
    use super::Validator;

    /// Formats the sum of two numbers as string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}
