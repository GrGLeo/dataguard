pub mod core;

// Re-export core validation rules from dataguard-core
pub use dataguard_core::rules::{
    IsInCheck,
    Monotonicity,
    NumericRule,
    // Numeric rules (note: these are generic in core)
    Range,
    RegexMatch,
    // String rules
    StringLengthCheck,
    // Traits
    StringRule,
    // Generic rules
    TypeCheck,
    UnicityCheck,
};
