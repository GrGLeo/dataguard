use crate::rules::generic::{TypeCheck, UnicityCheck};
use crate::rules::numeric::NumericRule;
use crate::rules::string::StringRule;
use arrow::datatypes::{Float64Type, Int64Type};

/// Internal enum to hold compiled, executable validation rules for each column type.
/// This is NOT exposed to Python - it's purely internal to the core validator.
pub enum ExecutableColumn {
    String {
        name: String,
        rules: Vec<Box<dyn StringRule>>,
        type_check: TypeCheck,
        unicity: Option<UnicityCheck>,
    },
    Integer {
        name: String,
        rules: Vec<Box<dyn NumericRule<Int64Type>>>,
        type_check: TypeCheck,
    },
    Float {
        name: String,
        rules: Vec<Box<dyn NumericRule<Float64Type>>>,
        type_check: TypeCheck,
    },
}

impl ExecutableColumn {
    pub fn get_name(&self) -> String {
        match self {
            ExecutableColumn::String { name, .. } => name.clone(),
            ExecutableColumn::Integer { name, .. } => name.clone(),
            ExecutableColumn::Float { name, .. } => name.clone(),
        }
    }
}
