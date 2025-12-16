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
    match rule.name.as_str() {
        "is_not_null" => {
            builder.is_not_null();
            Ok(())
        }
        "is_unique" => {
            builder.is_unique();
            Ok(())
        }
        "with_length_between" => {
            let min = rule.min_length.ok_or_else(|| CliError::MissingRuleField {
                rule_name: rule.name.clone(),
                column_name: column_name.to_string(),
                field_name: "min_length".to_string(),
            })?;
            let max = rule.max_length.ok_or_else(|| CliError::MissingRuleField {
                rule_name: rule.name.clone(),
                column_name: column_name.to_string(),
                field_name: "max_length".to_string(),
            })?;
            builder.with_length_between(min, max);
            Ok(())
        }
        "with_min_length" => {
            let min = rule.min_length.ok_or_else(|| CliError::MissingRuleField {
                rule_name: rule.name.clone(),
                column_name: column_name.to_string(),
                field_name: "min_length".to_string(),
            })?;
            builder.with_min_length(min);
            Ok(())
        }
        "with_max_length" => {
            let max = rule.max_length.ok_or_else(|| CliError::MissingRuleField {
                rule_name: rule.name.clone(),
                column_name: column_name.to_string(),
                field_name: "max_length".to_string(),
            })?;
            builder.with_max_length(max);
            Ok(())
        }
        "is_exact_length" => {
            let length = rule.length.ok_or_else(|| CliError::MissingRuleField {
                rule_name: rule.name.clone(),
                column_name: column_name.to_string(),
                field_name: "length".to_string(),
            })?;
            builder.is_exact_length(length);
            Ok(())
        }
        "is_in" => {
            let members = rule.members.ok_or_else(|| CliError::MissingRuleField {
                rule_name: rule.name.clone(),
                column_name: column_name.to_string(),
                field_name: "length".to_string(),
            })?;
            builder.is_in(members);
            Ok(())
        }
        "with_regex" => {
            let pattern = rule
                .pattern
                .as_ref()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name,
                    field_name: "pattern".to_string(),
                })?;
            let flags = &rule.flag;
            builder.with_regex(pattern.to_owned(), flags.to_owned())?;
            Ok(())
        }
        "is_numeric" => {
            builder.is_numeric()?;
            Ok(())
        }
        "is_alpha" => {
            builder.is_alpha()?;
            Ok(())
        }
        "is_alphanumeric" => {
            builder.is_alphanumeric()?;
            Ok(())
        }
        "is_uppercase" => {
            builder.is_uppercase()?;
            Ok(())
        }
        "is_lowercase" => {
            builder.is_lowercase()?;
            Ok(())
        }
        "is_url" => {
            builder.is_url()?;
            Ok(())
        }
        "is_email" => {
            builder.is_email()?;
            Ok(())
        }
        "is_uuid" => {
            builder.is_uuid()?;
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.name.clone(),
            column_type: "string".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

// TODO: we could refactor both numeric apply to a generic one. But we need to revisite
// dataguard-core
fn apply_integer_rule(
    builder: &mut IntegerColumnBuilder,
    rule: Rule,
    column_name: String,
) -> Result<(), CliError> {
    match rule.name.as_str() {
        "is_not_null" => {
            builder.is_not_null();
            Ok(())
        }
        "between" => {
            let min = rule
                .min
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "min_length".to_string(),
                })?;
            let i_min = match min {
                Value::Integer(i) => i,
                _ => {
                    return Err(CliError::WrongRuleData {
                        rule_name: rule.name.clone(),
                        column_name: column_name.to_string(),
                        field_type: "integer".to_string(),
                    })
                }
            };
            let max = rule
                .max
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "min_length".to_string(),
                })?;
            let i_max = match max {
                Value::Integer(i) => i,
                _ => {
                    return Err(CliError::WrongRuleData {
                        rule_name: rule.name.clone(),
                        column_name: column_name.to_string(),
                        field_type: "integer".to_string(),
                    })
                }
            };
            builder.between(i_min, i_max);
            Ok(())
        }
        "min" => {
            let min = rule
                .min
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "min".to_string(),
                })?;
            if let Value::Integer(i) = min {
                builder.min(i);
                Ok(())
            } else {
                Err(CliError::WrongRuleData {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_type: "integer".to_string(),
                })
            }
        }
        "max" => {
            let max = rule
                .max
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "max".to_string(),
                })?;
            if let Value::Integer(i) = max {
                builder.max(i);
                Ok(())
            } else {
                Err(CliError::WrongRuleData {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_type: "integer".to_string(),
                })
            }
        }
        "is_positive" => {
            builder.is_positive();
            Ok(())
        }
        "is_negative" => {
            builder.is_negative();
            Ok(())
        }
        "is_non_positive" => {
            builder.is_non_positive();
            Ok(())
        }
        "is_non_negative" => {
            builder.is_non_negative();
            Ok(())
        }
        "is_monotonically_increasing" => {
            builder.is_monotonically_increasing();
            Ok(())
        }
        "is_monotonically_descreasing" => {
            builder.is_monotonically_decreasing();
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.name.clone(),
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
    match rule.name.as_str() {
        "is_not_null" => {
            builder.is_not_null();
            Ok(())
        }
        "between" => {
            let min = rule
                .min
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "between".to_string(),
                })?;
            let i_min = match min {
                Value::Float(i) => i,
                _ => {
                    return Err(CliError::WrongRuleData {
                        rule_name: rule.name.clone(),
                        column_name: column_name.to_string(),
                        field_type: "float".to_string(),
                    })
                }
            };
            let max = rule
                .max
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "between".to_string(),
                })?;
            let i_max = match max {
                Value::Float(i) => i,
                _ => {
                    return Err(CliError::WrongRuleData {
                        rule_name: rule.name.clone(),
                        column_name: column_name.to_string(),
                        field_type: "float".to_string(),
                    })
                }
            };
            builder.between(i_min, i_max);
            Ok(())
        }
        "min" => {
            let min = rule
                .min
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "min".to_string(),
                })?;
            if let Value::Float(i) = min {
                builder.min(i);
                Ok(())
            } else {
                Err(CliError::WrongRuleData {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_type: "float".to_string(),
                })
            }
        }
        "max" => {
            let max = rule
                .max
                .to_owned()
                .ok_or_else(|| CliError::MissingRuleField {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_name: "max".to_string(),
                })?;
            if let Value::Float(i) = max {
                builder.max(i);
                Ok(())
            } else {
                Err(CliError::WrongRuleData {
                    rule_name: rule.name.clone(),
                    column_name: column_name.to_string(),
                    field_type: "float".to_string(),
                })
            }
        }
        "is_positive" => {
            builder.is_positive();
            Ok(())
        }
        "is_negative" => {
            builder.is_negative();
            Ok(())
        }
        "is_non_positive" => {
            builder.is_non_positive();
            Ok(())
        }
        "is_non_negative" => {
            builder.is_non_negative();
            Ok(())
        }
        "is_monotonically_increasing" => {
            builder.is_monotonically_increasing();
            Ok(())
        }
        "is_monotonically_descreasing" => {
            builder.is_monotonically_decreasing();
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.name.clone(),
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
    t.commit(all_builder)?;
    println!("{:?}", t.get_rules());
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a basic Rule with all fields set to None
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

    // ==================== STRING RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_string_rule_is_unique() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_unique");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_length_between() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("with_length_between");
        rule.min_length = Some(5);
        rule.max_length = Some(10);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_min_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("with_min_length");
        rule.min_length = Some(5);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_max_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("with_max_length");
        rule.max_length = Some(10);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_exact_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("is_exact_length");
        rule.length = Some(5);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_in() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("is_in");
        rule.members = Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_regex() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("with_regex");
        rule.pattern = Some("^[a-z]+$".to_string());
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_numeric() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_numeric");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_alpha() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_alpha");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_alphanumeric() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_alphanumeric");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_uppercase() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_uppercase");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_lowercase() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_lowercase");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_url() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_url");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_email() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_email");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_uuid() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_uuid");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_string_rule_unknown_rule() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("unknown_rule");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "unknown_rule");
                assert_eq!(column_type, "string");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_string_rule_with_length_between_missing_min() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("with_length_between");
        rule.max_length = Some(10);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::MissingRuleField {
                rule_name,
                column_name,
                field_name,
            } => {
                assert_eq!(rule_name, "with_length_between");
                assert_eq!(column_name, "test_col");
                assert_eq!(field_name, "min_length");
            }
            _ => panic!("Expected MissingRuleField error"),
        }
    }

    #[test]
    fn test_apply_string_rule_with_length_between_missing_max() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("with_length_between");
        rule.min_length = Some(5);
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::MissingRuleField {
                rule_name,
                column_name,
                field_name,
            } => {
                assert_eq!(rule_name, "with_length_between");
                assert_eq!(column_name, "test_col");
                assert_eq!(field_name, "max_length");
            }
            _ => panic!("Expected MissingRuleField error"),
        }
    }

    #[test]
    fn test_apply_string_rule_with_min_length_missing_field() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("with_min_length");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_string_rule_with_max_length_missing_field() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("with_max_length");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_string_rule_is_exact_length_missing_field() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_exact_length");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_string_rule_is_in_missing_field() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_in");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_string_rule_with_regex_missing_pattern() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = create_rule("with_regex");
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    // ==================== INTEGER RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_integer_rule_between() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.min = Some(Value::Integer(1));
        rule.max = Some(Value::Integer(10));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_min() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("min");
        rule.min = Some(Value::Integer(5));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_max() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("max");
        rule.max = Some(Value::Integer(100));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_positive() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_positive");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_negative() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_negative");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_non_positive() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_non_positive");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_non_negative() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_non_negative");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_monotonically_increasing() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_monotonically_increasing");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_monotonically_descreasing() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_monotonically_descreasing");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_integer_rule_unknown_rule() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("unknown_rule");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "unknown_rule");
                assert_eq!(column_type, "integer");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_integer_rule_between_missing_min() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.max = Some(Value::Integer(10));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_between_missing_max() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.min = Some(Value::Integer(1));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_between_wrong_type_min() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.min = Some(Value::String("not_an_integer".to_string()));
        rule.max = Some(Value::Integer(10));
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
        let mut rule = create_rule("between");
        rule.min = Some(Value::Integer(1));
        rule.max = Some(Value::String("not_an_integer".to_string()));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_min_missing_field() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("min");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_min_wrong_type() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("min");
        rule.min = Some(Value::Float(5.5));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_max_missing_field() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let rule = create_rule("max");
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_max_wrong_type() {
        let mut builder = IntegerColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("max");
        rule.max = Some(Value::Float(100.5));
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    // ==================== FLOAT RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_float_rule_between() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.min = Some(Value::Float(1.5));
        rule.max = Some(Value::Float(10.5));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_min() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("min");
        rule.min = Some(Value::Float(5.5));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_max() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("max");
        rule.max = Some(Value::Float(100.5));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_positive() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_positive");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_negative() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_negative");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_non_positive() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_non_positive");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_non_negative() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_non_negative");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_monotonically_increasing() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_monotonically_increasing");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_monotonically_descreasing() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("is_monotonically_descreasing");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_float_rule_unknown_rule() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("unknown_rule");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "unknown_rule");
                assert_eq!(column_type, "float");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_float_rule_between_missing_min() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.max = Some(Value::Float(10.5));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_between_missing_max() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.min = Some(Value::Float(1.5));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_between_wrong_type_min() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("between");
        rule.min = Some(Value::String("not_a_float".to_string()));
        rule.max = Some(Value::Float(10.5));
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
        let mut rule = create_rule("between");
        rule.min = Some(Value::Float(1.5));
        rule.max = Some(Value::String("not_a_float".to_string()));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_min_missing_field() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("min");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_min_wrong_type() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("min");
        rule.min = Some(Value::Integer(5));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_max_missing_field() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let rule = create_rule("max");
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_max_wrong_type() {
        let mut builder = FloatColumnBuilder::new("test_col".to_string());
        let mut rule = create_rule("max");
        rule.max = Some(Value::Integer(100));
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string());
        assert!(result.is_err());
    }
}
