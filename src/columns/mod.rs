use crate::rules::Rule;
use pyo3::prelude::*;

pub mod string_column;

pub trait ColumnBuilder: Send + Sync {
    fn build(self) -> Column;
}

#[pyclass]
pub struct Column {
    name: String,
    rules: Vec<Box<dyn Rule>>,
}

impl Column {
    pub fn new(name: String, rules: Vec<Box<dyn Rule>>) -> Self {
        Self { name, rules }
    }
}
