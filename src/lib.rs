use pyo3::prelude::*;
use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    sync::Arc,
};

use arrow::{
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema},
};
use pyo3::exceptions::PyValueError;

#[pyfunction]
fn count_csv_lines(path: &str) -> PyResult<usize> {
    let file = File::open(path).unwrap();
    // For now we treat every column as utf8
    let res = generate_utf_schema(path);
    match res {
        Ok(schema) => {
            if let Ok(reader) = ReaderBuilder::new(Arc::new(schema))
                .with_header(true)
                .build(file)
            {
                Ok(reader.map(|b| b.unwrap().num_rows()).sum())
            } else {
                Err(PyErr::new::<PyValueError, _>("CSV file is empty"))
            }
        }
        Err(_) => Err(PyErr::new::<PyValueError, _>("CSV file is empty")),
    }
}

#[pyclass]
struct Validator {
    columns: Vec<(String, DataType)>
}

#[pymethods]
impl Validator {
    #[new]
    fn new() -> Self {
        Self {
            columns: Vec::new()
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
            },
            "int" => {
                self.columns.push((name, DataType::Int64));
                Ok(())
            },
            _ => {
                Err(PyErr::new::<PyValueError, _>("Unknown column type"))
            }
        }
    }
}

fn generate_utf_schema(path: &str) -> Result<Schema, io::Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    if let Some(first) = lines.next() {
        let header = first?;
        let cols: Vec<&str> = header.split(',').collect();
        let fields: Vec<Field> = cols
            .iter()
            .map(|c| Field::new(c.trim(), DataType::Utf8, true))
            .collect();
        Ok(Schema::new(fields))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CSV file is empty",
        ))
    }
}

/// A Python module implemented in Rust.
#[pyo3::pymodule]
mod dataguard {
    use pyo3::prelude::*;

    #[pymodule_export]
    use super::count_csv_lines;
    #[pymodule_export]
    use super::Validator;

    /// Formats the sum of two numbers as string.
    #[pyfunction]
    fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
        Ok((a + b).to_string())
    }
}
