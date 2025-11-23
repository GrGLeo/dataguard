use pyo3::prelude::*;

/// An enum representing all possible validation rules.
#[pyclass(name = "Rule")]
#[derive(Clone)]
pub enum Rule {
    StringLength {
        min: Option<usize>,
        max: Option<usize>,
    },
    StringRegex {
        pattern: String,
        flag: Option<String>,
    },
}
