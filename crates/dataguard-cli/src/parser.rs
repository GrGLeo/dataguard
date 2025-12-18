use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use toml::Value;

use crate::errors::ConfigError;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub table: Vec<ConfigTable>,
}

#[derive(Debug, Deserialize)]
pub struct ConfigTable {
    pub name: String,
    pub path: String,
    pub column: Vec<Column>,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: String,
    pub rule: Vec<Rule>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "name", rename_all = "snake_case")]
pub enum Rule {
    // Generic rules
    IsUnique,
    IsNotNull,

    //String rules
    WithLengthBetween {
        min_length: usize,
        max_length: usize,
    },
    WithMinLength {
        min_length: usize,
    },
    WithMaxLength {
        max_length: usize,
    },
    IsExactLength {
        length: usize,
    },
    IsIn {
        members: Vec<String>,
    },
    WithRegex {
        pattern: String,
        flag: Option<String>,
    },
    IsNumeric,
    IsAlpha,
    #[serde(rename = "is_alphanumeric")]
    IsAlphaNumeric,
    #[serde(rename = "is_uppercase")]
    IsUpperCase,
    #[serde(rename = "is_lowercase")]
    IsLowerCase,
    IsUrl,
    IsEmail,
    IsUuid,

    // Numeric Rule
    Between {
        min: Value,
        max: Value,
    },
    Min {
        min: Value,
    },
    Max {
        max: Value,
    },
    IsPositive,
    IsNonPositive,
    IsNegative,
    IsNonNegative,
    IsIncreasing,
    IsDecreasing,
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rule::IsNotNull => write!(f, "is_not_null"),
            Rule::IsUnique => write!(f, "is_unique"),
            Rule::WithLengthBetween { .. } => write!(f, "with_length_between"),
            Rule::WithMinLength { .. } => write!(f, "with_min_length"),
            Rule::WithMaxLength { .. } => write!(f, "with_max_length"),
            Rule::IsExactLength { .. } => write!(f, "is_exact_length"),
            Rule::IsIn { .. } => write!(f, "is_in"),
            Rule::WithRegex { .. } => write!(f, "with_regex"),
            Rule::IsNumeric => write!(f, "is_numeric"),
            Rule::IsAlpha => write!(f, "is_alpha"),
            Rule::IsAlphaNumeric => write!(f, "is_alphanumeric"),
            Rule::IsUpperCase => write!(f, "is_uppercase"),
            Rule::IsLowerCase => write!(f, "is_lowercase"),
            Rule::IsUrl => write!(f, "is_url"),
            Rule::IsEmail => write!(f, "is_email"),
            Rule::IsUuid => write!(f, "is_uuid"),
            Rule::Between { .. } => write!(f, "between"),
            Rule::Min { .. } => write!(f, "min"),
            Rule::Max { .. } => write!(f, "max"),
            Rule::IsPositive => write!(f, "is_positive"),
            Rule::IsNonPositive => write!(f, "is_non_positive"),
            Rule::IsNegative => write!(f, "is_negative"),
            Rule::IsNonNegative => write!(f, "is_non_negative"),
            Rule::IsIncreasing => write!(f, "is_increasing"),
            Rule::IsDecreasing => write!(f, "is_decreasing"),
        }
    }
}

pub fn validate_config(config: &Config) -> Result<(), ConfigError> {
    for table in &config.table {
        if !Path::new(&table.path).exists() {
            return Err(ConfigError::FileNotFound {
                table_path: table.path.clone(),
            });
        }
        for column in &table.column {
            validate_column(column)?
        }
    }
    Ok(())
}

pub fn parse_config(path: String) -> Result<Config> {
    let config_path = std::path::PathBuf::from(path);
    let config_str = std::fs::read_to_string(config_path.clone())
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;
    let config: Config = toml::from_str(config_str.as_str())
        .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;
    if config.table.is_empty() {
        anyhow::bail!("Configuration file contains no table");
    }
    validate_config(&config)?;
    Ok(config)
}

fn validate_column(col: &Column) -> Result<(), ConfigError> {
    for rule in &col.rule {
        match rule {
            Rule::WithLengthBetween {
                min_length,
                max_length,
            } => {
                if min_length > max_length {
                    return Err(ConfigError::RuleError {
                        rule_name: "with_length_between".to_string(),
                        column_name: col.name.clone(),
                        message: format!(
                            "min length ({}) must be less than max length ({})",
                            min_length, max_length
                        ),
                    });
                }
                if min_length == max_length {
                    return Err(ConfigError::RuleError {
                        rule_name: "with_length_between".to_string(),
                        column_name: col.name.clone(),
                        message: format!(
                        "min length ({}) is equal max length ({})\n Hint: use 'is_exact_length'",
                        min_length, max_length
                    ),
                    });
                }
            }
            Rule::Between { min, max } => match (&min, &max) {
                (Value::Float(min_f), Value::Float(max_f)) => {
                    if min_f > max_f {
                        return Err(ConfigError::RuleError {
                            rule_name: "with_length_between".to_string(),
                            column_name: col.name.clone(),
                            message: format!(
                                "min value ({}) must be less than max value ({}) for float rule.",
                                min_f, max_f
                            ),
                        });
                    }
                    if min_f == max_f {
                        return Err(ConfigError::RuleError {
                         rule_name: "with_length_between".to_string(),
                         column_name: col.name.clone(),
                        message: format!("min value ({}) is equal max value ({})\n Hint: use 'is_exact_length'", min_f, max_f)
                     });
                    }
                }
                (Value::Integer(min_i), Value::Integer(max_i)) => {
                    if min_i > max_i {
                        return Err(ConfigError::RuleError {
                            rule_name: "between".to_string(),
                            column_name: col.name.clone(),
                            message: format!(
                                "min value ({}) must be less than max value ({}) for integer rule.",
                                min_i, max_i
                            ),
                        });
                    }
                    if min_i == max_i {
                        return Err(ConfigError::RuleError {
                         rule_name: "between".to_string(),
                         column_name: col.name.clone(),
                        message: format!("min value ({}) is equal max value ({})\n Hint: use 'is_exact_length'", min_i, max_i)
                     });
                    }
                }
                (Value::Integer(_), Value::Float(_)) | (Value::Float(_), Value::Integer(_)) => {
                    return Err(ConfigError::RuleError {
                    rule_name: "between".to_string(),
                    column_name: col.name.clone(),
                    message: format!("type mismatch min and max must be the same type (both integer or both float). Got min: {:?}, max: {:?}", min, max)
                });
                }
                _ => {
                    return Err(ConfigError::RuleError {
                    rule_name: "between".to_string(),
                    column_name: col.name.clone(),
                    message: format!("Unsupported type for min/max. Expected Integer or Float, got min: {:?}, max: {:?}", min, max)
                });
                }
            },
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use toml::Value;

    fn create_column(name: &str, rules: Vec<Rule>) -> Column {
        Column {
            name: name.to_string(),
            datatype: "string".to_string(),
            rule: rules,
        }
    }

    #[test]
    fn test_validate_column_min_max_length_valid() {
        let rule = Rule::WithLengthBetween {
            min_length: 3,
            max_length: 10,
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_min_greater_than_max_length() {
        let rule = Rule::WithLengthBetween {
            min_length: 10,
            max_length: 3,
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("min length (10) must be less than max length (3)"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_min_equal_max_length() {
        let rule = Rule::WithLengthBetween {
            min_length: 5,
            max_length: 5,
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("min length (5) is equal max length (5)"));
                assert!(message.contains("Hint: use 'is_exact_length'"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_integer_min_max_valid() {
        let rule = Rule::Between {
            min: Value::Integer(1),
            max: Value::Integer(10),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_integer_min_greater_than_max() {
        let rule = Rule::Between {
            min: Value::Integer(10),
            max: Value::Integer(1),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message
                    .contains("min value (10) must be less than max value (1) for integer rule"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_integer_min_equal_max() {
        let rule = Rule::Between {
            min: Value::Integer(5),
            max: Value::Integer(5),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("min value (5) is equal max value (5)"));
                assert!(message.contains("Hint: use 'is_exact_length'"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_integer_negative_values() {
        let rule = Rule::Between {
            min: Value::Integer(-10),
            max: Value::Integer(-1),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_float_min_max_valid() {
        let rule = Rule::Between {
            min: Value::Float(1.5),
            max: Value::Float(10.5),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_float_min_greater_than_max() {
        let rule = Rule::Between {
            min: Value::Float(10.5),
            max: Value::Float(1.5),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message
                    .contains("min value (10.5) must be less than max value (1.5) for float rule"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_float_min_equal_max() {
        let rule = Rule::Between {
            min: Value::Float(5.5),
            max: Value::Float(5.5),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("min value (5.5) is equal max value (5.5)"));
                assert!(message.contains("Hint: use 'is_exact_length'"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_float_negative_values() {
        let rule = Rule::Between {
            min: Value::Float(-10.5),
            max: Value::Float(-1.5),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_type_mismatch_int_float() {
        let rule = Rule::Between {
            min: Value::Integer(1),
            max: Value::Float(10.0),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("type mismatch"));
                assert!(message.contains("both integer or both float"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_type_mismatch_float_int() {
        let rule = Rule::Between {
            min: Value::Float(1.0),
            max: Value::Integer(10),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("type mismatch"));
                assert!(message.contains("both integer or both float"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_unsupported_type_string() {
        let rule = Rule::Between {
            min: Value::String("1".to_string()),
            max: Value::String("10".to_string()),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("Unsupported type for min/max"));
                assert!(message.contains("Expected Integer or Float"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_unsupported_type_boolean() {
        let rule = Rule::Between {
            min: Value::Boolean(true),
            max: Value::Boolean(false),
        };
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { message, .. } => {
                assert!(message.contains("Unsupported type for min/max"));
                assert!(message.contains("Expected Integer or Float"));
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_multiple_rules_all_valid() {
        let rule1 = Rule::WithLengthBetween {
            min_length: 3,
            max_length: 10,
        };

        let rule2 = Rule::Between {
            min: Value::Integer(1),
            max: Value::Integer(100),
        };

        let rule3 = Rule::Between {
            min: Value::Float(0.5),
            max: Value::Float(99.5),
        };

        let column = create_column("test_col", vec![rule1, rule2, rule3]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_multiple_rules_one_invalid() {
        let rule1 = Rule::WithLengthBetween {
            min_length: 3,
            max_length: 10,
        };

        let rule2 = Rule::Between {
            min: Value::Integer(100),
            max: Value::Integer(1), // Invalid: min > max
        };

        let rule3 = Rule::Between {
            min: Value::Float(0.5),
            max: Value::Float(99.5),
        };

        let column = create_column("test_col", vec![rule1, rule2, rule3]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { rule_name, .. } => {
                assert_eq!(rule_name, "between");
            }
            _ => panic!("Expected RuleError"),
        }
    }

    #[test]
    fn test_validate_column_empty_rules() {
        let column = create_column("test_col", vec![]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_no_constraints() {
        let rule = Rule::IsUnique;
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }
}
