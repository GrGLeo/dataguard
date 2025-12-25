use crate::{
    columns::{ColumnBuilder, ColumnRule, ColumnType},
    errors::RuleError,
};
use regex::Regex;

#[derive(Debug, Clone)]
pub struct StringColumnBuilder {
    name: String,
    type_threshold: Option<f64>,
    rules: Vec<ColumnRule>,
}

impl ColumnBuilder for StringColumnBuilder {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn type_threshold(&self) -> f64 {
        self.type_threshold.unwrap_or(0.)
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::String
    }

    fn rules(&self) -> &[ColumnRule] {
        self.rules.as_slice()
    }

    fn format(&self) -> Option<&str> {
        None
    }
}

impl StringColumnBuilder {
    pub fn new(name: String) -> Self {
        Self {
            name,
            type_threshold: None,
            rules: Vec::new(),
        }
    }

    /// Set the type checking threshold
    pub fn with_type_threshold(mut self, threshold: f64) -> Self {
        self.type_threshold = Some(threshold);
        self
    }

    /// Add not null constraint
    pub fn is_not_null(&mut self, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::NullCheck { threshold });
        self
    }

    /// Add uniqueness constraint
    pub fn is_unique(&mut self, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::Unicity { threshold });
        self
    }

    /// Set length constraints (both min and max)
    pub fn with_length_between(&mut self, min: usize, max: usize, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            name: "WithLengthBetween".to_string(),
            threshold,
            min: Some(min),
            max: Some(max),
        });
        self
    }

    /// Set minimum length
    pub fn with_min_length(&mut self, min: usize, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            name: "WithMinLength".to_string(),
            threshold,
            min: Some(min),
            max: None,
        });
        self
    }

    /// Set maximum length
    pub fn with_max_length(&mut self, max: usize, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            name: "WithMaxLength".to_string(),
            threshold,
            min: None,
            max: Some(max),
        });
        self
    }

    /// Set exact length
    pub fn is_exact_length(&mut self, len: usize, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::StringLength {
            name: "IsExactLength".to_string(),
            threshold,
            min: Some(len),
            max: Some(len),
        });
        self
    }

    /// Check if value is in a set of allowed values
    pub fn is_in(&mut self, members: Vec<String>, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::StringMembers {
            name: "IsIn".to_string(),
            threshold,
            members,
        });
        self
    }

    /// Match against a regex pattern
    pub fn with_regex(
        &mut self,
        pattern: String,
        flags: Option<String>,
        threshold: f64,
    ) -> Result<&mut Self, RuleError> {
        // Validate regex at build time
        Regex::new(&pattern).map_err(|e| {
            RuleError::ValidationError(format!("Invalid regex pattern '{}': {}", pattern, e))
        })?;
        self.rules.push(ColumnRule::StringRegex {
            name: "WithRegex".to_string(),
            threshold,
            pattern,
            flags,
        });
        Ok(self)
    }

    fn with_defined_regex(
        &mut self,
        name: String,
        pattern: String,
        flags: Option<String>,
        threshold: f64,
    ) -> Result<&mut Self, RuleError> {
        // Validate regex at build time
        Regex::new(&pattern).map_err(|e| {
            RuleError::ValidationError(format!("Invalid regex pattern '{}': {}", pattern, e))
        })?;
        self.rules.push(ColumnRule::StringRegex {
            name,
            threshold,
            pattern,
            flags,
        });
        Ok(self)
    }

    /// Check if string contains only numeric characters
    pub fn is_numeric(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsNumeric".to_string(),
            r"^\d+$".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string contains only alphabetic characters
    pub fn is_alpha(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsAlpha".to_string(),
            r"^[a-zA-Z]+$".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string contains only alphanumeric characters
    pub fn is_alphanumeric(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsAlphaNumeric".to_string(),
            r"^[a-zA-Z0-9]+$".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string is lowercase
    pub fn is_lowercase(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsLowerCase".to_string(),
            r"^[a-z0-9\s-]+$".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string is uppercase
    pub fn is_uppercase(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsUpperCase".to_string(),
            r"^[A-Z0-9\s-]+$".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string is a valid URL
    pub fn is_url(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsUrl".to_string(),
            r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string is a valid email
    pub fn is_email(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsEmail".to_string(),
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$".to_string(),
            None,
            threshold,
        )
    }

    /// Check if string is a valid UUID
    pub fn is_uuid(&mut self, threshold: f64) -> Result<&mut Self, RuleError> {
        self.with_defined_regex(
            "IsUuid".to_string(),
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
                .to_string(),
            None,
            threshold,
        )
    }
}
