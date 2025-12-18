use crate::{
    errors::CliError,
    parser::{ConfigTable, Rule},
};
use anyhow::{Context, Result};
use dataguard_core::{
    column::ColumnBuilder, CsvTable, FloatColumnBuilder, IntegerColumnBuilder, StringColumnBuilder,
    Table,
};
use toml::Value;

fn apply_string_rule(
    builder: &mut StringColumnBuilder,
    rule: Rule,
    column_name: String,
) -> Result<(), CliError> {
    match rule {
        Rule::IsUnique => {
            builder.is_unique();
            Ok(())
        }
        Rule::WithLengthBetween {
            min_length,
            max_length,
        } => {
            builder.with_length_between(min_length, max_length);
            Ok(())
        }
        Rule::WithMinLength { min_length } => {
            builder.with_min_length(min_length);
            Ok(())
        }
        Rule::WithMaxLength { max_length } => {
            builder.with_max_length(max_length);
            Ok(())
        }
        Rule::IsExactLength { length } => {
            builder.is_exact_length(length);
            Ok(())
        }
        Rule::IsIn { members } => {
            builder.is_in(members);
            Ok(())
        }
        Rule::WithRegex { pattern, flag } => {
            builder.with_regex(pattern.to_owned(), flag)?;
            Ok(())
        }
        Rule::IsNumeric => {
            builder.is_numeric()?;
            Ok(())
        }
        Rule::IsAlpha => {
            builder.is_alpha()?;
            Ok(())
        }
        Rule::IsAlphaNumeric => {
            builder.is_alphanumeric()?;
            Ok(())
        }
        Rule::IsUpperCase => {
            builder.is_uppercase()?;
            Ok(())
        }
        Rule::IsLowerCase => {
            builder.is_lowercase()?;
            Ok(())
        }
        Rule::IsUrl => {
            builder.is_url()?;
            Ok(())
        }
        Rule::IsEmail => {
            builder.is_email()?;
            Ok(())
        }
        Rule::IsUuid => {
            builder.is_uuid()?;
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.to_string(),
            column_type: "string".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

fn extract_integer(value: &Value, rule_name: String, column_name: String) -> Result<i64, CliError> {
    let i = match value {
        Value::Integer(i) => i,
        _ => {
            return Err(CliError::WrongRuleData {
                rule_name,
                column_name,
                field_type: "integer".to_string(),
            })
        }
    };
    Ok(*i)
}

fn extract_float(value: &Value, rule_name: String, column_name: String) -> Result<f64, CliError> {
    let f = match value {
        Value::Float(f) => f,
        _ => {
            return Err(CliError::WrongRuleData {
                rule_name,
                column_name,
                field_type: "float".to_string(),
            })
        }
    };
    Ok(*f)
}

fn apply_integer_rule(
    builder: &mut IntegerColumnBuilder,
    rule: Rule,
    column_name: String,
) -> Result<(), CliError> {
    match rule {
        Rule::Between { ref min, ref max } => {
            let i_min = extract_integer(min, rule.to_string(), column_name.clone())?;
            let i_max = extract_integer(max, rule.to_string(), column_name.clone())?;
            builder.between(i_min, i_max);
            Ok(())
        }
        Rule::Min { ref min } => {
            let i_min = extract_integer(min, rule.to_string(), column_name.clone())?;
            builder.min(i_min);
            Ok(())
        }
        Rule::Max { ref max } => {
            let i_max = extract_integer(max, rule.to_string(), column_name.clone())?;
            builder.max(i_max);
            Ok(())
        }
        Rule::IsPositive => {
            builder.is_positive();
            Ok(())
        }
        Rule::IsNegative => {
            builder.is_negative();
            Ok(())
        }
        Rule::IsNonPositive => {
            builder.is_non_positive();
            Ok(())
        }
        Rule::IsNonNegative => {
            builder.is_non_negative();
            Ok(())
        }
        Rule::IsIncreasing => {
            builder.is_monotonically_increasing();
            Ok(())
        }
        Rule::IsDecreasing => {
            builder.is_monotonically_decreasing();
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.to_string(),
            column_type: "integer".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

fn apply_float_rule(
    builder: &mut FloatColumnBuilder,
    rule: Rule,
    column_name: String,
) -> Result<(), CliError> {
    match rule {
        Rule::Between { ref min, ref max } => {
            let f_min = extract_float(min, rule.to_string(), column_name.clone())?;
            let f_max = extract_float(max, rule.to_string(), column_name.clone())?;
            builder.between(f_min, f_max);
            Ok(())
        }
        Rule::Min { ref min } => {
            let f_min = extract_float(min, rule.to_string(), column_name.clone())?;
            builder.min(f_min);
            Ok(())
        }
        Rule::Max { ref max } => {
            let f_max = extract_float(max, rule.to_string(), column_name.clone())?;
            builder.max(f_max);
            Ok(())
        }
        Rule::IsPositive => {
            builder.is_positive();
            Ok(())
        }
        Rule::IsNegative => {
            builder.is_negative();
            Ok(())
        }
        Rule::IsNonPositive => {
            builder.is_non_positive();
            Ok(())
        }
        Rule::IsNonNegative => {
            builder.is_non_negative();
            Ok(())
        }
        Rule::IsIncreasing => {
            builder.is_monotonically_increasing();
            Ok(())
        }
        Rule::IsDecreasing => {
            builder.is_monotonically_decreasing();
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.to_string(),
            column_type: "float".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

pub fn construct_csv_table(table: &ConfigTable) -> Result<CsvTable> {
    let path = &table.path;
    let mut all_builder: Vec<Box<dyn ColumnBuilder>> = Vec::new();
    for column in &table.column {
        match column.datatype.as_str() {
            "float" => {
                let mut builder = FloatColumnBuilder::new(column.name.clone());
                for rule in &column.rule {
                    apply_float_rule(&mut builder, rule.clone(), column.name.clone()).with_context(
                        || format!("Failed to apply rule to column '{}'", column.name.clone()),
                    )?
                }
                all_builder.push(Box::new(builder));
            }
            "integer" => {
                let mut builder = IntegerColumnBuilder::new(column.name.clone());
                for rule in &column.rule {
                    apply_integer_rule(&mut builder, rule.clone(), column.name.clone())
                        .with_context(|| {
                            format!("Failed to apply rule to column '{}'", column.name.clone())
                        })?
                }
                all_builder.push(Box::new(builder));
            }
            "string" => {
                let mut builder = StringColumnBuilder::new(column.name.clone());
                for rule in &column.rule {
                    apply_string_rule(&mut builder, rule.clone(), column.name.clone())
                        .with_context(|| {
                            format!("Failed to apply rule to column '{}'", column.name)
                        })?
                }
                all_builder.push(Box::new(builder));
            }
            _ => {
                return Err(CliError::UnknownDatatype {
                    datatype: column.datatype.clone(),
                    column_name: column.name.clone(),
                }
                .into());
            }
        }
    }
    let mut t = CsvTable::new(path.clone(), table.name.clone())
        .with_context(|| format!("Failed to create validation table: {}", table.name))?;
    t.commit(all_builder).unwrap();
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== STRING RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_string_rule_is_unique() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUnique;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_length_between() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithLengthBetween {
            min_length: 5,
            max_length: 10,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_min_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithMinLength { min_length: 5 };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_max_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithMaxLength { max_length: 10 };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_exact_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsExactLength { length: 5 };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_in() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsIn {
            members: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_regex() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithRegex {
            pattern: "^[a-z]+$".to_string(),
            flag: None,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_numeric() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNumeric;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_alpha() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsAlpha;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_alphanumeric() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsAlphaNumeric;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_uppercase() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUpperCase;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_lowercase() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsLowerCase;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_url() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUrl;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_email() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsEmail;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_uuid() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUuid;
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_string_rule_unknown_rule() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::Integer(1),
            max: Value::Integer(10),
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "between");
                assert_eq!(column_type, "string");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    // ==================== INTEGER RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_integer_rule_between() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::Integer(1),
            max: Value::Integer(10),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_min() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Min {
            min: Value::Integer(5),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_max() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Max {
            max: Value::Integer(100),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_positive() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsPositive;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_negative() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNegative;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_non_positive() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNonPositive;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_non_negative() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNonNegative;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_monotonically_increasing() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsIncreasing;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_monotonically_descreasing() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsDecreasing;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_integer_rule_unknown_rule() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUnique;
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "is_unique");
                assert_eq!(column_type, "integer");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_integer_rule_between_wrong_type_min() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::String("not_an_integer".to_string()),
            max: Value::Integer(10),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::WrongRuleData {
                rule_name,
                column_name,
                field_type,
            } => {
                assert_eq!(rule_name, "between");
                assert_eq!(column_name, "test_col");
                assert_eq!(field_type, "integer");
            }
            _ => panic!("Expected WrongRuleData error"),
        }
    }

    #[test]
    fn test_apply_integer_rule_between_wrong_type_max() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::Integer(1),
            max: Value::String("not_an_integer".to_string()),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_min_wrong_type() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Min {
            min: Value::Float(5.5),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_max_wrong_type() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = Rule::Max {
            max: Value::Float(100.5),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    // ==================== FLOAT RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_float_rule_between() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::Float(1.5),
            max: Value::Float(10.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_min() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Min {
            min: Value::Float(5.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_max() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Max {
            max: Value::Float(100.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_positive() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsPositive;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_negative() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNegative;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_non_positive() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNonPositive;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_non_negative() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNonNegative;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_monotonically_increasing() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsIncreasing;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_monotonically_descreasing() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsDecreasing;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_float_rule_unknown_rule() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUnique;
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "is_unique");
                assert_eq!(column_type, "float");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_float_rule_between_wrong_type_min() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::String("not_a_float".to_string()),
            max: Value::Float(10.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::WrongRuleData {
                rule_name,
                column_name,
                field_type,
            } => {
                assert_eq!(rule_name, "between");
                assert_eq!(column_name, "test_col");
                assert_eq!(field_type, "float");
            }
            _ => panic!("Expected WrongRuleData error"),
        }
    }

    #[test]
    fn test_apply_float_rule_between_wrong_type_max() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            min: Value::Float(1.5),
            max: Value::String("not_a_float".to_string()),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_min_wrong_type() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Min {
            min: Value::Integer(5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_max_wrong_type() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = Rule::Max {
            max: Value::Integer(100),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }
}
