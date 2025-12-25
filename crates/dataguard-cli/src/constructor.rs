use crate::{
    errors::CliError,
    parser::{ConfigTable, Relation, Rule},
};
use anyhow::{Context, Result};
use dataguard_core::{
    columns::{date_builder::DateColumnBuilder, relation_builder::RelationBuilder, ColumnBuilder},
    utils::operator::CompOperator,
    CsvTable, NumericColumnBuilder, ParquetTable, StringColumnBuilder, Table,
};
use toml::Value;

fn apply_string_rule(
    builder: &mut StringColumnBuilder,
    rule: Rule,
    column_name: String,
    rule_threshold: f64,
) -> Result<(), CliError> {
    match rule {
        Rule::IsNotNull { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_not_null(t);
            Ok(())
        }
        Rule::IsUnique { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_unique(t);
            Ok(())
        }
        Rule::WithLengthBetween {
            threshold,
            min_length,
            max_length,
        } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.with_length_between(min_length, max_length, t);
            Ok(())
        }
        Rule::WithMinLength {
            threshold,
            min_length,
        } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.with_min_length(min_length, t);
            Ok(())
        }
        Rule::WithMaxLength {
            threshold,
            max_length,
        } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.with_max_length(max_length, t);
            Ok(())
        }
        Rule::IsExactLength { threshold, length } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_exact_length(length, t);
            Ok(())
        }
        Rule::IsIn { threshold, members } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_in(members, t);
            Ok(())
        }
        Rule::WithRegex {
            threshold,
            pattern,
            flag,
        } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.with_regex(pattern.to_owned(), flag, t)?;
            Ok(())
        }
        Rule::IsNumeric { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_numeric(t)?;
            Ok(())
        }
        Rule::IsAlpha { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_alpha(t)?;
            Ok(())
        }
        Rule::IsAlphaNumeric { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_alphanumeric(t)?;
            Ok(())
        }
        Rule::IsUpperCase { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_uppercase(t)?;
            Ok(())
        }
        Rule::IsLowerCase { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_lowercase(t)?;
            Ok(())
        }
        Rule::IsUrl { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_url(t)?;
            Ok(())
        }
        Rule::IsEmail { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_email(t)?;
            Ok(())
        }
        Rule::IsUuid { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_uuid(t)?;
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
    builder: &mut NumericColumnBuilder<i64>,
    rule: Rule,
    column_name: String,
    rule_threshold: f64,
) -> Result<(), CliError> {
    match rule {
        Rule::IsNotNull { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_not_null(t);
            Ok(())
        }
        Rule::IsUnique { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_unique(t);
            Ok(())
        }
        Rule::Between {
            threshold,
            ref min,
            ref max,
        } => {
            let i_min = extract_integer(min, rule.to_string(), column_name.clone())?;
            let i_max = extract_integer(max, rule.to_string(), column_name.clone())?;
            let t = threshold.unwrap_or(rule_threshold);
            builder.between(i_min, i_max, t);
            Ok(())
        }
        Rule::Min { threshold, ref min } => {
            let i_min = extract_integer(min, rule.to_string(), column_name.clone())?;
            let t = threshold.unwrap_or(rule_threshold);
            builder.min(i_min, t);
            Ok(())
        }
        Rule::Max { threshold, ref max } => {
            let i_max = extract_integer(max, rule.to_string(), column_name.clone())?;
            let t = threshold.unwrap_or(rule_threshold);
            builder.max(i_max, t);
            Ok(())
        }
        Rule::IsPositive { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_positive(t);
            Ok(())
        }
        Rule::IsNegative { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_negative(t);
            Ok(())
        }
        Rule::IsNonPositive { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_non_positive(t);
            Ok(())
        }
        Rule::IsNonNegative { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_non_negative(t);
            Ok(())
        }
        Rule::IsIncreasing { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_monotonically_increasing(t);
            Ok(())
        }
        Rule::IsDecreasing { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_monotonically_decreasing(t);
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
    builder: &mut NumericColumnBuilder<f64>,
    rule: Rule,
    column_name: String,
    rule_threshold: f64,
) -> Result<(), CliError> {
    match rule {
        Rule::IsNotNull { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_not_null(t);
            Ok(())
        }
        Rule::IsUnique { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_unique(t);
            Ok(())
        }
        Rule::Between {
            threshold,
            ref min,
            ref max,
        } => {
            let f_min = extract_float(min, rule.to_string(), column_name.clone())?;
            let f_max = extract_float(max, rule.to_string(), column_name.clone())?;
            let t = threshold.unwrap_or(rule_threshold);
            builder.between(f_min, f_max, t);
            Ok(())
        }
        Rule::Min { threshold, ref min } => {
            let f_min = extract_float(min, rule.to_string(), column_name.clone())?;
            let t = threshold.unwrap_or(rule_threshold);
            builder.min(f_min, t);
            Ok(())
        }
        Rule::Max { threshold, ref max } => {
            let f_max = extract_float(max, rule.to_string(), column_name.clone())?;
            let t = threshold.unwrap_or(rule_threshold);
            builder.max(f_max, t);
            Ok(())
        }
        Rule::IsPositive { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_positive(t);
            Ok(())
        }
        Rule::IsNegative { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_negative(t);
            Ok(())
        }
        Rule::IsNonPositive { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_non_positive(t);
            Ok(())
        }
        Rule::IsNonNegative { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_non_negative(t);
            Ok(())
        }
        Rule::IsIncreasing { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_monotonically_increasing(t);
            Ok(())
        }
        Rule::IsDecreasing { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_monotonically_decreasing(t);
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.to_string(),
            column_type: "float".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

fn apply_date_rule(
    builder: &mut DateColumnBuilder,
    rule: Rule,
    column_name: String,
    rule_threshold: f64,
) -> Result<(), CliError> {
    match rule {
        Rule::IsNotNull { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_not_null(t);
            Ok(())
        }
        Rule::IsUnique { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_unique(t);
            Ok(())
        }
        Rule::IsAfter {
            threshold,
            year,
            month,
            day,
        } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_after(year, month, day, t);
            Ok(())
        }
        Rule::IsBefore {
            threshold,
            year,
            month,
            day,
        } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_before(year, month, day, t);
            Ok(())
        }
        Rule::IsNotFutur { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_not_futur(t);
            Ok(())
        }
        Rule::IsNotPast { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_not_past(t);
            Ok(())
        }
        Rule::IsWeekday { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_weekday(t);
            Ok(())
        }
        Rule::IsWeekend { threshold } => {
            let t = threshold.unwrap_or(rule_threshold);
            builder.is_weekend(t);
            Ok(())
        }
        _ => Err(CliError::UnknownRule {
            rule_name: rule.to_string(),
            column_type: "string".to_string(),
            column_name: column_name.to_string(),
        }),
    }
}

fn apply_relation_rule(
    builder: &mut RelationBuilder,
    rule: Relation,
    relation_threshold: f64,
) -> Result<(), CliError> {
    match rule {
        Relation::DateComparaison {
            threshold,
            operator,
        } => {
            let op = CompOperator::try_from(operator.as_str())?;
            let t = threshold.unwrap_or(relation_threshold);
            builder.date_comparaison(op, t);
            Ok(())
        }
    }
}

pub fn construct_csv_table(table: &ConfigTable) -> Result<Box<dyn Table>> {
    let path = &table.path;
    let global_type_threshold = &table.type_checking_threshold.unwrap_or(0.);
    let global_rule_threshold = &table.rule_threshold.unwrap_or(0.);
    println!("t: {}", global_rule_threshold);
    let mut all_column_builder: Vec<Box<dyn ColumnBuilder>> = Vec::new();
    let mut all_relation_builder: Vec<RelationBuilder> = Vec::new();
    for column in &table.column {
        let column_type_threshold = column
            .type_checking_threshold
            .unwrap_or(*global_type_threshold);
        let column_rule_threshold = column.rule_threshold.unwrap_or(*global_rule_threshold);
        match column.datatype.as_str() {
            "float" => {
                let mut builder = NumericColumnBuilder::<f64>::new(column.name.clone())
                    .with_type_threshold(column_type_threshold);
                for rule in &column.rule {
                    apply_float_rule(
                        &mut builder,
                        rule.clone(),
                        column.name.clone(),
                        column_rule_threshold,
                    )
                    .with_context(|| {
                        format!("Failed to apply rule to column '{}'", column.name.clone())
                    })?
                }
                all_column_builder.push(Box::new(builder));
            }
            "integer" => {
                let mut builder = NumericColumnBuilder::<i64>::new(column.name.clone())
                    .with_type_threshold(column_type_threshold);
                for rule in &column.rule {
                    apply_integer_rule(
                        &mut builder,
                        rule.clone(),
                        column.name.clone(),
                        column_rule_threshold,
                    )
                    .with_context(|| {
                        format!("Failed to apply rule to column '{}'", column.name.clone())
                    })?
                }
                all_column_builder.push(Box::new(builder));
            }
            "string" => {
                let mut builder = StringColumnBuilder::new(column.name.clone())
                    .with_type_threshold(column_type_threshold);
                for rule in &column.rule {
                    apply_string_rule(
                        &mut builder,
                        rule.clone(),
                        column.name.clone(),
                        column_rule_threshold,
                    )
                    .with_context(|| format!("Failed to apply rule to column '{}'", column.name))?
                }
                all_column_builder.push(Box::new(builder));
            }
            "date" => {
                let mut builder =
                    DateColumnBuilder::new(column.name.clone(), column.format.clone().unwrap())
                        .with_type_threshold(column_type_threshold);
                for rule in &column.rule {
                    apply_date_rule(
                        &mut builder,
                        rule.clone(),
                        column.name.clone(),
                        column_rule_threshold,
                    )
                    .with_context(|| format!("Failed to apply rule to column '{}'", column.name))?
                }
                all_column_builder.push(Box::new(builder));
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

    if let Some(relations) = &table.relations {
        for relation in relations {
            let relation_rule_threshold = relation.rule_threshold.unwrap_or(*global_rule_threshold);
            let mut builder =
                RelationBuilder::new([relation.column_one.clone(), relation.column_two.clone()]);
            for rule in &relation.rule {
                apply_relation_rule(&mut builder, rule.clone(), relation_rule_threshold)
                    .with_context(|| {
                        format!(
                            "Failed to apply rule to relation '{}' '{}'",
                            relation.column_one.clone(),
                            relation.column_two.clone()
                        )
                    })?
            }
            all_relation_builder.push(builder);
        }
    }
    match table.path.split_once(".") {
        Some((_, format)) => match format {
            "csv" => {
                let mut t = CsvTable::new(path.clone(), table.name.clone()).with_context(|| {
                    format!("Failed to create validation table: {}", table.name)
                })?;
                t.prepare(all_column_builder, all_relation_builder).unwrap();
                Ok(Box::new(t))
            }
            "parquet" => {
                let mut t =
                    ParquetTable::new(path.clone(), table.name.clone()).with_context(|| {
                        format!("Failed to create validation table: {}", table.name)
                    })?;
                t.prepare(all_column_builder, all_relation_builder).unwrap();
                Ok(Box::new(t))
            }
            _ => Err(CliError::UnknownFormat {
                format: format.to_string(),
            }
            .into()),
        },
        None => Err(CliError::UnknownFilePath {
            path: table.path.to_string(),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== STRING RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_string_rule_is_unique() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUnique { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_length_between() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithLengthBetween {
            threshold: None,
            min_length: 5,
            max_length: 10,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_min_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithMinLength {
            threshold: None,
            min_length: 5,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_max_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithMaxLength {
            threshold: None,
            max_length: 10,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_exact_length() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsExactLength {
            threshold: None,
            length: 5,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_in() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsIn {
            threshold: None,
            members: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_with_regex() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithRegex {
            threshold: None,
            pattern: "^[a-z]+$".to_string(),
            flag: None,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_numeric() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNumeric { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_alpha() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsAlpha { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_alphanumeric() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsAlphaNumeric { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_uppercase() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUpperCase { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_lowercase() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsLowerCase { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_url() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUrl { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_email() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsEmail { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_string_rule_is_uuid() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUuid { threshold: None };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_string_rule_unknown_rule() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::Integer(1),
            max: Value::Integer(10),
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 0.0);
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
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::Integer(1),
            max: Value::Integer(10),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_min() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Min {
            threshold: None,
            min: Value::Integer(5),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_max() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Max {
            threshold: None,
            max: Value::Integer(100),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_positive() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsPositive { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_negative() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsNegative { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_non_positive() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsNonPositive { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_non_negative() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsNonNegative { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_monotonically_increasing() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsIncreasing { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_integer_rule_is_monotonically_descreasing() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsDecreasing { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_integer_rule_unknown_rule() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::IsAlphaNumeric { threshold: None };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "is_alphanumeric");
                assert_eq!(column_type, "integer");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_integer_rule_between_wrong_type_min() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::String("not_an_integer".to_string()),
            max: Value::Integer(10),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
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
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::Integer(1),
            max: Value::String("not_an_integer".to_string()),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_min_wrong_type() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Min {
            threshold: None,
            min: Value::Float(5.5),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_integer_rule_max_wrong_type() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Max {
            threshold: None,
            max: Value::Float(100.5),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
    }

    // ==================== FLOAT RULE TESTS ====================

    // Success cases
    #[test]
    fn test_apply_float_rule_between() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::Float(1.5),
            max: Value::Float(10.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_min() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Min {
            threshold: None,
            min: Value::Float(5.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_max() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Max {
            threshold: None,
            max: Value::Float(100.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_positive() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsPositive { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_negative() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsNegative { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_non_positive() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsNonPositive { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_non_negative() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsNonNegative { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_monotonically_increasing() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsIncreasing { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_float_rule_is_monotonically_descreasing() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsDecreasing { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_ok());
    }

    // Error cases
    #[test]
    fn test_apply_float_rule_unknown_rule() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::IsAlphaNumeric { threshold: None };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::UnknownRule {
                rule_name,
                column_type,
                column_name,
            } => {
                assert_eq!(rule_name, "is_alphanumeric");
                assert_eq!(column_type, "float");
                assert_eq!(column_name, "test_col");
            }
            _ => panic!("Expected UnknownRule error"),
        }
    }

    #[test]
    fn test_apply_float_rule_between_wrong_type_min() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::String("not_a_float".to_string()),
            max: Value::Float(10.5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
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
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: None,
            min: Value::Float(1.5),
            max: Value::String("not_a_float".to_string()),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_min_wrong_type() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Min {
            threshold: None,
            min: Value::Integer(5),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_float_rule_max_wrong_type() {
        let mut builder = NumericColumnBuilder::<f64>::new("test_col".to_string());
        let rule = Rule::Max {
            threshold: None,
            max: Value::Integer(100),
        };
        let result = apply_float_rule(&mut builder, rule, "test_col".to_string(), 0.0);
        assert!(result.is_err());
    }

    // ============================================================================
    // Threshold Propagation Tests
    // ============================================================================

    #[test]
    fn test_threshold_propagation_rule_level() {
        // Test that a rule-level threshold is used
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithMinLength {
            threshold: Some(5.0), // Rule-level threshold
            min_length: 3,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 10.0);
        assert!(result.is_ok());

        // Verify the rule was added with the rule-level threshold (5.0, not 10.0)
        let rules = builder.rules();
        assert_eq!(rules.len(), 1);
        match &rules[0] {
            dataguard_core::ColumnRule::StringLength { threshold, .. } => {
                assert_eq!(*threshold, 5.0);
            }
            _ => panic!("Expected StringLength rule"),
        }
    }

    #[test]
    fn test_threshold_propagation_fallback_to_default() {
        // Test that when rule threshold is None, it falls back to the default
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::WithMinLength {
            threshold: None, // No rule-level threshold
            min_length: 3,
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 15.0);
        assert!(result.is_ok());

        // Verify the rule was added with the fallback threshold (15.0)
        let rules = builder.rules();
        assert_eq!(rules.len(), 1);
        match &rules[0] {
            dataguard_core::ColumnRule::StringLength { threshold, .. } => {
                assert_eq!(*threshold, 15.0);
            }
            _ => panic!("Expected StringLength rule"),
        }
    }

    #[test]
    fn test_threshold_propagation_numeric_rule() {
        let mut builder = NumericColumnBuilder::<i64>::new("test_col".to_string());
        let rule = Rule::Between {
            threshold: Some(8.5),
            min: Value::Integer(0),
            max: Value::Integer(100),
        };
        let result = apply_integer_rule(&mut builder, rule, "test_col".to_string(), 20.0);
        assert!(result.is_ok());

        let rules = builder.rules();
        assert_eq!(rules.len(), 1);
        match &rules[0] {
            dataguard_core::ColumnRule::NumericRange { threshold, .. } => {
                assert_eq!(*threshold, 8.5);
            }
            _ => panic!("Expected NumericRange rule"),
        }
    }

    #[test]
    fn test_threshold_propagation_null_check() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsNotNull {
            threshold: Some(2.0),
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 10.0);
        assert!(result.is_ok());

        let rules = builder.rules();
        assert_eq!(rules.len(), 1);
        match &rules[0] {
            dataguard_core::ColumnRule::NullCheck { threshold } => {
                assert_eq!(*threshold, 2.0);
            }
            _ => panic!("Expected NullCheck rule"),
        }
    }

    #[test]
    fn test_threshold_propagation_unicity() {
        let mut builder = StringColumnBuilder::new("test_col".to_string());
        let rule = Rule::IsUnique {
            threshold: Some(3.5),
        };
        let result = apply_string_rule(&mut builder, rule, "test_col".to_string(), 10.0);
        assert!(result.is_ok());

        let rules = builder.rules();
        assert_eq!(rules.len(), 1);
        match &rules[0] {
            dataguard_core::ColumnRule::Unicity { threshold } => {
                assert_eq!(*threshold, 3.5);
            }
            _ => panic!("Expected Unicity rule"),
        }
    }
}
