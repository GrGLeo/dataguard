pub mod columns;
pub mod rules;
pub mod validator;

// Re-export core modules
pub use dataguard_core::{errors, reader, report, types};

use crate::columns::float_column::FloatColumnBuilder;
use crate::columns::integer_column::IntegerColumnBuilder;
use crate::columns::string_column::StringColumnBuilder;
use crate::columns::Column;
use pyo3::prelude::*;

/// Creates a builder for defining rules on a string column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     StringColumnBuilder: A builder object for chaining rules.
#[pyfunction]
fn string_column(name: String) -> PyResult<StringColumnBuilder> {
    Ok(StringColumnBuilder::new(name))
}

/// Creates a builder for defining rules on a integer column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     IntegerColumnBuilder: A builder object for chaining rules.
#[pyfunction]
fn integer_column(name: String) -> PyResult<IntegerColumnBuilder> {
    Ok(IntegerColumnBuilder::new(name))
}

/// Creates a builder for defining rules on a float column.
///
/// Args:
///     name (str): The name of the column.
///
/// Returns:
///     FloatColumnBuilder: A builder object for chaining rules.
#[pyfunction]
fn float_column(name: String) -> PyResult<FloatColumnBuilder> {
    Ok(FloatColumnBuilder::new(name))
}

/// DataGuard: A high-performance CSV validation library.
#[pymodule]
fn dataguard(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<validator::Validator>()?;
    m.add_class::<Column>()?;
    m.add_class::<StringColumnBuilder>()?;
    m.add_class::<IntegerColumnBuilder>()?;
    m.add_class::<FloatColumnBuilder>()?;
    m.add_function(wrap_pyfunction!(string_column, m)?)?;
    m.add_function(wrap_pyfunction!(integer_column, m)?)?;
    m.add_function(wrap_pyfunction!(float_column, m)?)?;
    Ok(())
}
