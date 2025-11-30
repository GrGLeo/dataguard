use crate::rules::generic_rules::{TypeCheck, UnicityCheck};
use crate::rules::numeric_rules::NumericRule;
use crate::rules::string_rules::StringRule;
use arrow::datatypes::{Float64Type, Int64Type};

/// An internal enum to hold the compiled, logic-bearing validation rules for each column type.
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
