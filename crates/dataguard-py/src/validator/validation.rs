use dataguard_core::Validator as CoreValidator;
use pyo3::{exceptions::PyIOError, prelude::*, types::PyAny};

use crate::tables::{CsvTable, ParquetTable};

/// Python wrapper for Validator from dataguard-core.
///
/// The Validator manages multiple tables and provides methods to validate them.
#[pyclass(name = "Validator")]
pub struct Validator {
    inner: CoreValidator,
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl Validator {
    /// Create a new Validator instance.
    #[new]
    pub fn new() -> Self {
        Self {
            inner: CoreValidator::new(),
        }
    }

    /// Add a table to the validator.
    ///
    /// The table must be configured (via `prepare()`) before being added.
    ///
    /// Args:
    ///     name (str): Identifier for the table.
    ///     table: Configured table instance (CsvTable or ParquetTable).
    ///
    /// Returns:
    ///     None
    pub fn add_table(&mut self, name: String, table: Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(csv_table) = table.extract::<CsvTable>() {
            self.inner.add_table(name, Box::new(csv_table.inner));
        } else if let Ok(parquet_table) = table.extract::<ParquetTable>() {
            self.inner
                .add_table(name, Box::new(parquet_table.inner));
        } else {
            return Err(PyIOError::new_err(format!(
                "Invalid table type: {:?}",
                table
            )));
        }
        Ok(())
    }

    /// Validate a specific table by name.
    ///
    /// Args:
    ///     name (str): The name used when adding the table via `add_table()`.
    ///
    /// Returns:
    ///     dict: A dictionary containing validation results with keys:
    ///         - 'table_name': Name of the table
    ///         - 'total_rows': Total number of rows processed
    ///         - 'passed': Tuple of (passed_rules, total_rules)
    pub fn validate_table(&mut self, name: String) -> PyResult<PyObject> {
        let result = self
            .inner
            .validate_table(name)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Python::attach(|py| {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("table_name", &result.table_name)?;
            dict.set_item("total_rows", result.total_rows)?;
            let (passed, total) = result.is_passed();
            dict.set_item("passed", (passed, total))?;
            Ok(dict.into())
        })
    }

    /// Validate all tables in the validator.
    ///
    /// Returns:
    ///     list: A list of dictionaries, one for each table, containing validation results.
    pub fn validate_all(&mut self) -> PyResult<Vec<PyObject>> {
        let results = self
            .inner
            .validate_all()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        Python::attach(|py| {
            results
                .into_iter()
                .map(|result| {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("table_name", &result.table_name)?;
                    dict.set_item("total_rows", result.total_rows)?;
                    let (passed, total) = result.is_passed();
                    dict.set_item("passed", (passed, total))?;
                    Ok(dict.into())
                })
                .collect()
        })
    }
}
