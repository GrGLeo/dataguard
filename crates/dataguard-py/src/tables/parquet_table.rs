use dataguard_core::{ParquetTable as CoreParquetTable, Table as CoreTable};
use pyo3::{exceptions::PyIOError, prelude::*, types::PyAny};

use crate::columns::{
    DateColumnBuilder, FloatColumnBuilder, IntegerColumnBuilder, StringColumnBuilder,
};
use crate::relations::RelationBuilder;

/// Python wrapper for ParquetTable from dataguard-core.
///
/// A ParquetTable represents a Parquet file to be validated with specific column rules.
#[pyclass(name = "ParquetTable")]
pub struct ParquetTable {
    pub(crate) inner: CoreParquetTable,
}

#[pymethods]
impl ParquetTable {
    /// Create a new ParquetTable instance.
    ///
    /// Args:
    ///     path (str): Path to the Parquet file.
    ///     table_name (str): Name identifier for this table.
    ///
    /// Returns:
    ///     ParquetTable: A new ParquetTable instance.
    #[new]
    pub fn new(path: String, table_name: String) -> PyResult<Self> {
        let inner = CoreParquetTable::new(path, table_name)
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Prepare the table with column rules and relations.
    ///
    /// This method compiles the column builders and relations into executable rules.
    ///
    /// Args:
    ///     columns (list): List of column builders (StringColumnBuilder, IntegerColumnBuilder, etc.)
    ///     relations (list): Optional list of relation builders (default: [])
    ///
    /// Returns:
    ///     None
    #[pyo3(signature = (columns, relations=vec![]))]
    pub fn prepare(
        &mut self,
        columns: Vec<Bound<'_, PyAny>>,
        relations: Vec<Py<RelationBuilder>>,
    ) -> PyResult<()> {
        Python::attach(|py| {
            // Convert Python column builders to Rust ColumnBuilder trait objects
            let mut core_columns: Vec<Box<dyn dataguard_core::columns::ColumnBuilder>> = Vec::new();

            for py_col in columns {
                let builder: Box<dyn dataguard_core::columns::ColumnBuilder> =
                    if let Ok(col) = py_col.extract::<StringColumnBuilder>() {
                        col.to_core_column_builder()
                            .map_err(|e| PyIOError::new_err(e.to_string()))?
                    } else if let Ok(col) = py_col.extract::<IntegerColumnBuilder>() {
                        col.to_core_column_builder()
                            .map_err(|e| PyIOError::new_err(e.to_string()))?
                    } else if let Ok(col) = py_col.extract::<FloatColumnBuilder>() {
                        col.to_core_column_builder()
                            .map_err(|e| PyIOError::new_err(e.to_string()))?
                    } else if let Ok(col) = py_col.extract::<DateColumnBuilder>() {
                        col.to_core_column_builder()
                            .map_err(|e| PyIOError::new_err(e.to_string()))?
                    } else {
                        return Err(PyIOError::new_err(format!(
                            "Invalid column type: {:?}",
                            py_col
                        )));
                    };
                core_columns.push(builder);
            }

            // Convert Python relation builders to Rust RelationBuilder
            let core_relations: Vec<dataguard_core::columns::relation_builder::RelationBuilder> =
                relations
                    .into_iter()
                    .map(|py_rel| {
                        let rel = py_rel.borrow(py);
                        rel.to_core_relation_builder()
                    })
                    .collect();

            // Call prepare on the core table
            self.inner
                .prepare(core_columns, core_relations)
                .map_err(|e| PyIOError::new_err(e.to_string()))
        })
    }

    /// Validate the Parquet file against the prepared rules.
    ///
    /// Returns:
    ///     dict: A dictionary containing validation results with keys:
    ///         - 'table_name': Name of the table
    ///         - 'total_rows': Total number of rows processed
    ///         - 'passed': Tuple of (passed_rules, total_rules)
    pub fn validate(&mut self) -> PyResult<Py<PyAny>> {
        let result = self
            .inner
            .validate()
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
}
