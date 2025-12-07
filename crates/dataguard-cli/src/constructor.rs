use crate::{
    errors::CliError,
    parser::{Rule, Table},
};
use anyhow::{Context, Result};
use dataguard_core::{
    column::ColumnBuilder, FloatColumnBuilder, IntegerColumnBuilder, StringColumnBuilder, Validator,
};
use toml::Value;

fn apply_string_rule(
    builder: &mut StringColumnBuilder,
    rule: Rule,
    column_name: String,
) -> Result<(), CliError> {
    match rule.name.as_str() {
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
            column_type: "string".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

pub fn construct_validator(table: &Table) -> Result<()> {
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
    let mut v = Validator::new();
    v.commit(all_builder).unwrap();
    let _ = v.validate_csv(path, true);
    Ok(())
}
