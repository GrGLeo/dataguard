use crate::rules::core::Rule;
use pyo3::prelude::*;

pub mod string_column;

#[pyclass(name = "Column")]
#[derive(Clone)]
pub struct Column {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub column_type: String, // Using a simple String is easier for PyO3 interop
    #[pyo3(get)]
    pub rules: Vec<Rule>,
}

#[pymethods]
impl Column {
    #[new]
    pub fn new(name: String, column_type: String, rules: Vec<Rule>) -> Self {
        Self {
            name,
            column_type,
            rules,
        }
    }
}

#[derive(Clone)]
pub enum ColumnType {
    String,
    Integer,
}
