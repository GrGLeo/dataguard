use crate::rules::generic::{TypeCheck, UnicityCheck};
use crate::rules::numeric::NumericRule;
use crate::rules::string::StringRule;
use crate::rules::NullCheck;
use arrow::datatypes::{Float64Type, Int64Type};

/// Internal enum to hold compiled, executable validation rules for each column type.
pub enum ExecutableColumn {
    String {
        name: String,
        rules: Vec<Box<dyn StringRule>>,
        type_check: TypeCheck,
        unicity: Option<UnicityCheck>,
        null_check: Option<NullCheck>,
    },
    Integer {
        name: String,
        rules: Vec<Box<dyn NumericRule<Int64Type>>>,
        type_check: TypeCheck,
        null_check: Option<NullCheck>,
    },
    Float {
        name: String,
        rules: Vec<Box<dyn NumericRule<Float64Type>>>,
        type_check: TypeCheck,
        null_check: Option<NullCheck>,
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
