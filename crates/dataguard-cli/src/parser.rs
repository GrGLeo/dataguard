use std::path::Path;

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
pub struct Rule {
    pub name: String,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub length: Option<usize>,
    pub members: Option<Vec<String>>,
    pub pattern: Option<String>,
    pub flag: Option<String>,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

pub fn validate_config(config: &Config) -> Result<(), ConfigError> {
    for table in &config.table {
        if !Path::new(&table.path).exists() {
            return Err(ConfigError::FileNotFound {
                table_path: table.path.clone(),
            });
        }
        for column in &table.column {
            if let Err(e) = validate_column(column) {
                return Err(e);
            }
        }
    }
    Ok(())
}

fn validate_column(col: &Column) -> Result<(), ConfigError> {
    for rule in &col.rule {
        match (rule.min_length, rule.max_length) {
            (Some(min), Some(max)) => {
                if min > max {
                    return Err(ConfigError::RuleError {
                        rule_name: rule.name.clone(),
                        column_name: col.name.clone(),
                        message: format!(
                            "min length ({}) must be less than max length ({})",
                            min, max
                        ),
                    });
                }
                if min == max {
                    return Err(ConfigError::RuleError {
                        rule_name: rule.name.clone(),
                        column_name: col.name.clone(),
                        message: format!("min length ({}) is equal max length ({})\n Hint: use 'is_exact_length'", min, max)
                    });
                }
            }
            (_, _) => {}
        }

        match (&rule.min, &rule.max) {
            (Some(i), Some(j)) => {
                match (&i, &j) {
                    (Value::Float(min_f), Value::Float(max_f)) => {
                        if min_f > max_f {
                            return Err(ConfigError::RuleError {
                             rule_name: rule.name.clone(),
                             column_name: col.name.clone(),
                             message: format!("min value ({}) must be less than max value ({}) for float rule.", min_f, max_f)
                         });
                        }
                        if min_f == max_f {
                            return Err(ConfigError::RuleError {
                             rule_name: rule.name.clone(),
                             column_name: col.name.clone(),
                            message: format!("min value ({}) is equal max value ({})\n Hint: use 'is_exact_length'", min_f, max_f)
                         });
                        }
                    }
                    (Value::Integer(min_i), Value::Integer(max_i)) => {
                        if min_i > max_i {
                            return Err(ConfigError::RuleError {
                             rule_name: rule.name.clone(),
                             column_name: col.name.clone(),
                             message: format!("min value ({}) must be less than max value ({}) for integer rule.", min_i, max_i)
                         });
                        }
                        if min_i == max_i {
                            return Err(ConfigError::RuleError {
                             rule_name: rule.name.clone(),
                             column_name: col.name.clone(),
                            message: format!("min value ({}) is equal max value ({})\n Hint: use 'is_exact_length'", min_i, max_i)
                         });
                        }
                    }
                    (Value::Integer(_), Value::Float(_)) | (Value::Float(_), Value::Integer(_)) => {
                        return Err(ConfigError::RuleError {
                        rule_name: rule.name.clone(),
                        column_name: col.name.clone(),
                        message: format!("type mismatch min and max must be the same type (both integer or both float). Got min: {:?}, max: {:?}", i, j)
                    });
                    }
                    _ => {
                        return Err(ConfigError::RuleError {
                        rule_name: rule.name.clone(),
                        column_name: col.name.clone(),
                        message: format!("Unsupported type for min/max. Expected Integer or Float, got min: {:?}, max: {:?}", i, j)
                    });
                    }
                }
            }
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

    fn create_rule(name: &str) -> Rule {
        Rule {
            name: name.to_string(),
            min_length: None,
            max_length: None,
            length: None,
            members: None,
            pattern: None,
            flag: None,
            min: None,
            max: None,
        }
    }

    #[test]
    fn test_validate_column_min_max_length_valid() {
        let mut rule = create_rule("test_rule");
        rule.min_length = Some(3);
        rule.max_length = Some(10);
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_min_greater_than_max_length() {
        let mut rule = create_rule("test_rule");
        rule.min_length = Some(10);
        rule.max_length = Some(3);
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
        let mut rule = create_rule("test_rule");
        rule.min_length = Some(5);
        rule.max_length = Some(5);
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Integer(1));
        rule.max = Some(Value::Integer(10));
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_integer_min_greater_than_max() {
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Integer(10));
        rule.max = Some(Value::Integer(1));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Integer(5));
        rule.max = Some(Value::Integer(5));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Integer(-10));
        rule.max = Some(Value::Integer(-1));
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_float_min_max_valid() {
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Float(1.5));
        rule.max = Some(Value::Float(10.5));
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_float_min_greater_than_max() {
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Float(10.5));
        rule.max = Some(Value::Float(1.5));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Float(5.5));
        rule.max = Some(Value::Float(5.5));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Float(-10.5));
        rule.max = Some(Value::Float(-1.5));
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_type_mismatch_int_float() {
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Integer(1));
        rule.max = Some(Value::Float(10.0));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Float(1.0));
        rule.max = Some(Value::Integer(10));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::String("1".to_string()));
        rule.max = Some(Value::String("10".to_string()));
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
        let mut rule = create_rule("test_rule");
        rule.min = Some(Value::Boolean(true));
        rule.max = Some(Value::Boolean(false));
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
        let mut rule1 = create_rule("rule1");
        rule1.min_length = Some(3);
        rule1.max_length = Some(10);

        let mut rule2 = create_rule("rule2");
        rule2.min = Some(Value::Integer(1));
        rule2.max = Some(Value::Integer(100));

        let mut rule3 = create_rule("rule3");
        rule3.min = Some(Value::Float(0.5));
        rule3.max = Some(Value::Float(99.5));

        let column = create_column("test_col", vec![rule1, rule2, rule3]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_multiple_rules_one_invalid() {
        let mut rule1 = create_rule("rule1");
        rule1.min_length = Some(3);
        rule1.max_length = Some(10);

        let mut rule2 = create_rule("rule2");
        rule2.min = Some(Value::Integer(100));
        rule2.max = Some(Value::Integer(1)); // Invalid: min > max

        let mut rule3 = create_rule("rule3");
        rule3.min = Some(Value::Float(0.5));
        rule3.max = Some(Value::Float(99.5));

        let column = create_column("test_col", vec![rule1, rule2, rule3]);

        let result = validate_column(&column);
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ConfigError::RuleError { rule_name, .. } => {
                assert_eq!(rule_name, "rule2");
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
        let rule = create_rule("test_rule");
        let column = create_column("test_col", vec![rule]);

        let result = validate_column(&column);
        assert!(result.is_ok());
    }
}
