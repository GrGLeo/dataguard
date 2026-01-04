pub mod date_builder;
pub mod numeric_builder;
pub mod relation_builder;
pub mod string_builder;

#[cfg(test)]
mod columns_test;

use core::f64;

use crate::utils::operator::CompOperator;

pub trait ColumnBuilder {
    fn name(&self) -> &str;
    fn column_type(&self) -> ColumnType;
    fn rules(&self) -> &[ColumnRule];
    fn type_threshold(&self) -> f64;

    // For now only used for date, could be usefull for thousand separator in numeric or decimal
    // precision etc..
    fn format(&self) -> Option<&str>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    String,
    Integer,
    Float,
    DateType,
}

pub trait NumericType: Copy + Send + Sync {
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
        name: String,
        threshold: f64,
        min: Option<usize>,
        max: Option<usize>,
    },
    StringRegex {
        name: String,
        threshold: f64,
        pattern: String,
        flags: Option<String>,
    },
    StringMembers {
        name: String,
        threshold: f64,
        members: Vec<String>,
    },

    // Numeric rules (works for both Integer and Float)
    NumericRange {
        name: String,
        threshold: f64,
        min: Option<f64>,
        max: Option<f64>,
    },

    Monotonicity {
        name: String,
        threshold: f64,
        ascending: bool,
    },

    // Statistical rules (require stats computation)
    StdDevCheck {
        name: String,
        threshold: f64,
        max_std_dev: f64,
    },
    MeanVariance {
        name: String,
        threshold: f64,
        max_variance_percent: f64,
    },

    // Date rules (works only for Date32 for now)
    DateBoundary {
        name: String,
        threshold: f64,
        after: bool,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
    },

    WeekDay {
        name: String,
        threshold: f64,
        is_week: bool,
    },

    // Generic rules
    Unicity {
        threshold: f64,
    },
    NullCheck {
        threshold: f64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    DateComparaison { op: CompOperator, threshold: f64 },
    NumericComparaison { op: CompOperator, threshold: f64 },
}
