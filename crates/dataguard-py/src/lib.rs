pub mod columns;
pub mod relations;
pub mod tables;

#[allow(unused_imports)]
use columns::{
    date_column, float_column, integer_column, string_column, DateColumnBuilder,
    FloatColumnBuilder, IntegerColumnBuilder, StringColumnBuilder,
};
use pyo3::prelude::*;
#[allow(unused_imports)]
use relations::{relation, RelationBuilder};
use tables::{CsvTable, ParquetTable};

/// DataGuard: A high-performance data validation library.
///
/// This module provides Python bindings for the dataguard-core library,
/// enabling fast validation of CSV and Parquet files with a fluent builder API.
#[pymodule]
fn dataguard(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register classes
    m.add_class::<CsvTable>()?;
    m.add_class::<ParquetTable>()?;
    m.add_class::<StringColumnBuilder>()?;
    m.add_class::<IntegerColumnBuilder>()?;
    m.add_class::<FloatColumnBuilder>()?;
    m.add_class::<DateColumnBuilder>()?;
    m.add_class::<RelationBuilder>()?;

    // Register column builder functions
    m.add_function(wrap_pyfunction!(columns::string_column, m)?)?;
    m.add_function(wrap_pyfunction!(columns::integer_column, m)?)?;
    m.add_function(wrap_pyfunction!(columns::float_column, m)?)?;
    m.add_function(wrap_pyfunction!(columns::date_column, m)?)?;
    m.add_function(wrap_pyfunction!(relations::relation, m)?)?;

    Ok(())
}
