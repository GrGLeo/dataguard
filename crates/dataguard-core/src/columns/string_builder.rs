use crate::{
    columns::{ColumnBuilder, ColumnRule, ColumnType},
    errors::RuleError,
};
use regex::Regex;

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

    fn format(&self) -> Option<&str> {
        None
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
