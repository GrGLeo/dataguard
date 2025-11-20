pub mod column_builder;
pub mod errors;
pub mod reader;
pub mod rules;
pub mod types;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arrow::array::RecordBatch;
use pyo3::{exceptions::PyIOError, prelude::*};

use crate::{column_builder::ColumnBuilder, reader::read_csv, types::RuleMap};

#[pyclass]
struct Validator {
    rules: Arc<Mutex<RuleMap>>,
    batches: Vec<Arc<RecordBatch>>,
}

#[pymethods]
impl Validator {
    #[new]
    fn new() -> Self {
        Self {
            rules: Arc::new(Mutex::new(HashMap::new())),
            batches: Vec::new(),
        }
    }

    fn add_column_rule(&self, column_name: &str) -> PyResult<ColumnBuilder> {
        Ok(ColumnBuilder::new(
            column_name.to_string(),
            Arc::clone(&self.rules),
        ))
    }

    fn validate_csv(&mut self, path: &str) -> PyResult<usize> {
        if let Ok(batches) = read_csv(path) {
            self.batches = batches;
            Ok(self.batches.iter().map(|b| b.num_rows()).sum())
        } else {
            Err(PyErr::new::<PyIOError, _>("Failed to load CSV"))
        }
    }

    fn get_rules(&self) -> PyResult<HashMap<String, Vec<String>>> {
        let rules = self.rules.lock().unwrap();
        let mut result = HashMap::new();
        for (column, rule_list) in rules.iter() {
            let names: Vec<String> = rule_list.iter().map(|r| r.name().to_string()).collect();
            result.insert(column.clone(), names);
        }
        Ok(result)
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
