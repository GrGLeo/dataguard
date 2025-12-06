pub mod core;

// Re-export core validation rules from dataguard-core
pub use dataguard_core::rules::{
    // Traits
    StringRule, NumericRule,
    // String rules
    StringLengthCheck, RegexMatch, IsInCheck,
    // Numeric rules (note: these are generic in core)
    Range, Monotonicity,
    // Generic rules
    TypeCheck, UnicityCheck,
};
