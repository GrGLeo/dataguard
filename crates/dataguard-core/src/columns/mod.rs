pub mod date_builder;
pub mod numeric_builder;
pub mod string_builder;

#[cfg(test)]
mod columns_test;

use core::f64;

pub trait ColumnBuilder {
    fn name(&self) -> &str;
    fn column_type(&self) -> ColumnType;
    fn rules(&self) -> &[ColumnRule];
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    String,
    Integer,
    Float,
    DateType,
}

pub trait NumericType: Copy {
    fn column_type() -> ColumnType;
    fn to_f64(self) -> f64;
    fn positive_threshold() -> f64;
    fn negative_threshold() -> f64;
}

impl NumericType for i64 {
    fn column_type() -> ColumnType {
        ColumnType::Integer
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn positive_threshold() -> f64 {
        1.
    }
    fn negative_threshold() -> f64 {
        -1.
    }
}

impl NumericType for f64 {
    fn column_type() -> ColumnType {
        ColumnType::Float
    }
    fn to_f64(self) -> f64 {
        self
    }
    fn positive_threshold() -> f64 {
        f64::EPSILON
    }
    fn negative_threshold() -> f64 {
        -f64::EPSILON
    }
}

/// Rule enum representing all possible validation rules
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnRule {
    // String rules
    StringLength {
        min: Option<usize>,
        max: Option<usize>,
    },
    StringRegex {
        pattern: String,
        flags: Option<String>,
    },
    StringMembers {
        members: Vec<String>,
    },

    // Numeric rules (works for both Integer and Float)
    NumericRange {
        min: Option<f64>,
        max: Option<f64>,
    },
    Monotonicity {
        ascending: bool,
    },

    // Date rules (works only for Date32 for now)
    DateBoundary {
        after: bool,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
    },

    // Generic rules
    Unicity,
    NullCheck,
}
