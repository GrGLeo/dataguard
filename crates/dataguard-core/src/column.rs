use crate::errors::RuleError;
use core::f64;
use regex::Regex;
use std::marker::PhantomData;

pub trait ColumnBuilder {
    fn name(&self) -> &str;
    fn column_type(&self) -> ColumnType;
    fn rules(&self) -> &[ColumnRule];
}

/// Column type enum (no PyO3 pollution)
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    String,
    Integer,
    Float,
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

    // Generic rules
    Unicity,
    NullCheck,
}

/// Builder for String columns
#[derive(Debug, Clone)]
pub struct StringColumnBuilder {
    name: String,
    rules: Vec<ColumnRule>,
}

impl ColumnBuilder for StringColumnBuilder {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::String
    }

    fn rules(&self) -> &[ColumnRule] {
        self.rules.as_slice()
    }
}

impl StringColumnBuilder {
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
        }
    }

    /// Add not null constraint
    pub fn is_not_null(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NullCheck);
        self
    }

    /// Add uniqueness constraint
    pub fn is_unique(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::Unicity);
        self
    }

    /// Set length constraints (both min and max)
    pub fn with_length_between(&mut self, min: usize, max: usize) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            min: Some(min),
            max: Some(max),
        });
        self
    }

    /// Set minimum length
    pub fn with_min_length(&mut self, min: usize) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            min: Some(min),
            max: None,
        });
        self
    }

    /// Set maximum length
    pub fn with_max_length(&mut self, max: usize) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            min: None,
            max: Some(max),
        });
        self
    }

    /// Set exact length
    pub fn is_exact_length(&mut self, len: usize) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            min: Some(len),
            max: Some(len),
        });
        self
    }

    /// Check if value is in a set of allowed values
    pub fn is_in(&mut self, members: Vec<String>) -> &mut Self {
        self.rules.push(ColumnRule::StringMembers { members });
        self
    }

    /// Match against a regex pattern
    pub fn with_regex(
        &mut self,
        pattern: String,
        flags: Option<String>,
    ) -> Result<&mut Self, RuleError> {
        // Validate regex at build time
        Regex::new(&pattern).map_err(|e| {
            RuleError::ValidationError(format!("Invalid regex pattern '{}': {}", pattern, e))
        })?;
        self.rules.push(ColumnRule::StringRegex { pattern, flags });
        Ok(self)
    }

    /// Check if string contains only numeric characters
    pub fn is_numeric(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(r"^\d+$".to_string(), None)
    }

    /// Check if string contains only alphabetic characters
    pub fn is_alpha(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(r"^[a-zA-Z]+$".to_string(), None)
    }

    /// Check if string contains only alphanumeric characters
    pub fn is_alphanumeric(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(r"^[a-zA-Z0-9]+$".to_string(), None)
    }

    /// Check if string is lowercase
    pub fn is_lowercase(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(r"^[a-z0-9\s-]+$".to_string(), None)
    }

    /// Check if string is uppercase
    pub fn is_uppercase(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(r"^[A-Z0-9\s-]+$".to_string(), None)
    }

    /// Check if string is a valid URL
    pub fn is_url(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}".to_string(), None)
    }

    /// Check if string is a valid email
    pub fn is_email(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$".to_string(),
            None,
        )
    }

    /// Check if string is a valid UUID
    pub fn is_uuid(&mut self) -> Result<&mut Self, RuleError> {
        self.with_regex(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
                .to_string(),
            None,
        )
    }
}

#[derive(Debug, Clone)]
pub struct NumericColumnBuilder<T: NumericType> {
    name: String,
    rules: Vec<ColumnRule>,
    _phantom: PhantomData<T>,
}

impl<T: NumericType> ColumnBuilder for NumericColumnBuilder<T> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn column_type(&self) -> ColumnType {
        T::column_type()
    }

    fn rules(&self) -> &[ColumnRule] {
        self.rules.as_slice()
    }
}

impl<T: NumericType> NumericColumnBuilder<T> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Add not null constraint
    pub fn is_not_null(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NullCheck);
        self
    }

    /// Set numeric range (both min and max)
    pub fn between(&mut self, min: T, max: T) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(min.to_f64()),
            max: Some(max.to_f64()),
        });
        self
    }

    /// Set minimum value
    pub fn min(&mut self, min: T) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(min.to_f64()),
            max: None,
        });
        self
    }

    /// Set maximum value
    pub fn max(&mut self, max: T) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: None,
            max: Some(max.to_f64()),
        });
        self
    }

    /// Check if values are positive (> 0)
    pub fn is_positive(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(T::positive_threshold()),
            max: None,
        });
        self
    }

    /// Check if values are negative (< 0)
    pub fn is_negative(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: None,
            max: Some(T::negative_threshold()),
        });
        self
    }

    /// Check if values are non-negative (>= 0)
    pub fn is_non_negative(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(0.0),
            max: None,
        });
        self
    }

    /// Check if values are non-positive (<= 0)
    pub fn is_non_positive(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: None,
            max: Some(0.0),
        });
        self
    }

    /// Check if values are monotonically increasing
    pub fn is_monotonically_increasing(&mut self) -> &mut Self {
        self.rules
            .push(ColumnRule::Monotonicity { ascending: true });
        self
    }

    /// Check if values are monotonically decreasing
    pub fn is_monotonically_decreasing(&mut self) -> &mut Self {
        self.rules
            .push(ColumnRule::Monotonicity { ascending: false });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_column_builder() {
        let mut builder = StringColumnBuilder::new("name".to_string());
        builder.with_min_length(3).with_max_length(50);

        assert_eq!(builder.name(), "name");
        assert_eq!(builder.column_type(), ColumnType::String);
        assert_eq!(builder.rules().len(), 2);
    }

    #[test]
    fn test_string_column_with_regex() {
        let mut builder = StringColumnBuilder::new("email".to_string());
        builder.is_email().unwrap();

        assert_eq!(builder.column_type(), ColumnType::String);
        assert_eq!(builder.rules().len(), 1);
        match &builder.rules()[0] {
            ColumnRule::StringRegex { pattern, .. } => {
                assert!(pattern.contains("@"));
            }
            _ => panic!("Expected StringRegex rule"),
        }
    }

    #[test]
    fn test_string_column_invalid_regex() {
        let mut builder = StringColumnBuilder::new("test".to_string());
        let result = builder.with_regex("[invalid(".to_string(), None);

        assert!(result.is_err());
    }

    #[test]
    fn test_integer_column_builder() {
        let mut builder = NumericColumnBuilder::<i64>::new("age".to_string());
        builder.between(0, 120);

        assert_eq!(builder.name(), "age");
        assert_eq!(builder.column_type(), ColumnType::Integer);
        assert_eq!(builder.rules().len(), 1);
    }

    #[test]
    fn test_integer_column_is_positive() {
        let mut builder = NumericColumnBuilder::<i64>::new("count".to_string());
        builder.is_positive();

        match &builder.rules()[0] {
            ColumnRule::NumericRange { min, max } => {
                assert_eq!(min, &Some(1.0));
                assert_eq!(max, &None);
            }
            _ => panic!("Expected NumericRange rule"),
        }
    }

    #[test]
    fn test_float_column_builder() {
        let mut builder = NumericColumnBuilder::<f64>::new("price".to_string());
        builder.between(0.0, 1000.0);

        assert_eq!(builder.name(), "price");
        assert_eq!(builder.column_type(), ColumnType::Float);
        assert_eq!(builder.rules().len(), 1);
    }

    #[test]
    fn test_float_column_monotonicity() {
        let mut builder = NumericColumnBuilder::<f64>::new("timestamp".to_string());
        builder.is_monotonically_increasing();

        match &builder.rules()[0] {
            ColumnRule::Monotonicity { ascending } => {
                assert!(ascending);
            }
            _ => panic!("Expected Monotonicity rule"),
        }
    }

    #[test]
    fn test_column_chaining() {
        let mut builder = StringColumnBuilder::new("username".to_string());
        builder
            .with_min_length(3)
            .with_max_length(20)
            .is_alphanumeric()
            .unwrap()
            .is_unique();

        assert_eq!(builder.rules().len(), 4);
    }
}
