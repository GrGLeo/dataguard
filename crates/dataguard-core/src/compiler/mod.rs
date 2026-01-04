//! Rule compilation module.
//!
//! Converts high-level `ColumnBuilder` with `ColumnRule` enums into executable
//! `ExecutableColumn` with trait objects for runtime validation.
//!
//! This module is used by table implementations (CsvTable, future ParquetTable, etc.)
//! to compile user-defined validation rules into optimized, type-specific validators.

use std::{collections::HashMap, fmt::Debug};

use arrow::datatypes::{DataType, Date32Type, Float64Type, Int64Type};
use arrow_array::ArrowNumericType;
use num_traits::{Num, NumCast};

#[cfg(test)]
mod tests;

use crate::{
    columns::{relation_builder::RelationBuilder, ColumnBuilder, NumericType, TableConstraint},
    rules::{
        date::{DateBoundaryCheck, DateRule, DateTypeCheck},
        numeric::{MeanVarianceCheck, StdDevCheck},
        relations::{CompareCheck, RelationRule},
        IsInCheck, Monotonicity, NullCheck, NumericRule, Range, RegexMatch, StringLengthCheck,
        StringRule, TypeCheck, UnicityCheck, WeekDayCheck,
    },
    validator::{ExecutableColumn, ExecutableRelation},
    ColumnRule, ColumnType, RuleError,
};

/// Compile string column rules into executable validators.
///
/// Separates domain rules (length, regex, membership) from meta-rules (unicity, null check).
/// Returns an error if a non-string rule is found.
#[allow(clippy::type_complexity)]
fn compile_string_rules(
    rules: &[ColumnRule],
    column_name: &str,
) -> Result<
    (
        Vec<Box<dyn StringRule>>,
        Option<UnicityCheck>,
        Option<NullCheck>,
    ),
    RuleError,
> {
    let mut executable_rules: Vec<Box<dyn StringRule>> = Vec::new();
    let mut unicity_check = None;
    let mut null_check = None;

    for rule in rules {
        match rule {
            ColumnRule::StringLength {
                name,
                threshold,
                min,
                max,
            } => {
                executable_rules.push(Box::new(StringLengthCheck::new(
                    name.clone(),
                    *threshold,
                    *min,
                    *max,
                )));
            }
            ColumnRule::StringRegex {
                name,
                threshold,
                pattern,
                flags,
            } => {
                executable_rules.push(Box::new(RegexMatch::new(
                    name.clone(),
                    *threshold,
                    pattern.clone(),
                    flags.clone(),
                )));
            }
            ColumnRule::StringMembers {
                name,
                threshold,
                members,
            } => {
                executable_rules.push(Box::new(IsInCheck::new(
                    name.clone(),
                    *threshold,
                    members.to_vec(),
                )));
            }
            ColumnRule::Unicity { threshold } => {
                unicity_check = Some(UnicityCheck::new(*threshold));
            }
            ColumnRule::NullCheck { threshold } => {
                null_check = Some(NullCheck::new(*threshold));
            }
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "Invalid rule {:?} for String column '{}'",
                    rule, column_name,
                )))
            }
        }
    }
    Ok((executable_rules, unicity_check, null_check))
}

#[allow(clippy::type_complexity)]
fn compile_date_rules(
    rules: &[ColumnRule],
    column_name: &str,
) -> Result<
    (
        Vec<Box<dyn DateRule>>,
        Option<UnicityCheck>,
        Option<NullCheck>,
    ),
    RuleError,
> {
    let mut executable_rules: Vec<Box<dyn DateRule>> = Vec::new();
    let mut unicity_check = None;
    let mut null_check = None;

    for rule in rules {
        match rule {
            ColumnRule::Unicity { threshold } => {
                unicity_check = Some(UnicityCheck::new(*threshold));
            }
            ColumnRule::NullCheck { threshold } => {
                null_check = Some(NullCheck::new(*threshold));
            }
            ColumnRule::DateBoundary {
                name,
                threshold,
                after,
                year,
                month,
                day,
            } => {
                let rule =
                    DateBoundaryCheck::new(name.clone(), *threshold, *after, *year, *month, *day)?;
                executable_rules.push(Box::new(rule));
            }
            ColumnRule::WeekDay {
                name,
                threshold,
                is_week,
            } => {
                let rule = WeekDayCheck::new(name.clone(), *threshold, *is_week);
                executable_rules.push(Box::new(rule));
            }
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "Invalid rule {:?} for Date column '{}'",
                    rule, column_name,
                )))
            }
        }
    }
    Ok((executable_rules, unicity_check, null_check))
}

/// Compile numeric column rules into executable validators.
///
/// Generic over both the native type (N) and Arrow type (A) to support
/// both integer and floating-point columns. Separates domain rules (range,
/// monotonicity) from meta-rules (unicity, null check).
/// Returns an error if a non-numeric rule is found.
#[allow(clippy::type_complexity)]
fn compile_numeric_rules<N, A>(
    rules: &[ColumnRule],
    column_name: &str,
) -> Result<
    (
        Vec<Box<dyn NumericRule<A>>>, // domain_rules
        Vec<Box<dyn NumericRule<A>>>, // stats_rules
        Option<UnicityCheck>,
        Option<NullCheck>,
    ),
    RuleError,
>
where
    N: NumericType + Num + PartialOrd + Debug + NumCast + 'static,
    A: ArrowNumericType<Native = N>,
{
    let mut unicity = None;
    let mut null_rule: Option<NullCheck> = None;
    let mut domain_rules: Vec<Box<dyn NumericRule<A>>> = Vec::new();
    let mut stats_rules: Vec<Box<dyn NumericRule<A>>> = Vec::new();
    for rule in rules {
        match rule {
            ColumnRule::NumericRange {
                name,
                threshold,
                min,
                max,
            } => {
                let min_conv = min.and_then(|v| N::from(v));
                let max_conv = max.and_then(|v| N::from(v));
                domain_rules.push(Box::new(Range::<N>::new(
                    name.clone(),
                    *threshold,
                    min_conv,
                    max_conv,
                )));
            }
            ColumnRule::Monotonicity {
                name,
                ascending,
                threshold: treshold,
            } => {
                domain_rules.push(Box::new(Monotonicity::<N>::new(
                    name.clone(),
                    *treshold,
                    *ascending,
                )));
            }
            ColumnRule::StdDevCheck {
                name,
                threshold,
                max_std_dev,
            } => {
                stats_rules.push(Box::new(StdDevCheck::<N>::new(
                    name.clone(),
                    *threshold,
                    *max_std_dev,
                )));
            }
            ColumnRule::MeanVariance {
                name,
                threshold,
                max_variance_percent,
            } => {
                stats_rules.push(Box::new(MeanVarianceCheck::<N>::new(
                    name.clone(),
                    *threshold,
                    *max_variance_percent,
                )));
            }
            ColumnRule::NullCheck { threshold } => null_rule = Some(NullCheck::new(*threshold)),
            ColumnRule::Unicity { threshold } => {
                unicity = Some(UnicityCheck::new(*threshold));
            }
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "Invalid rule {:?} for numeric column '{}'",
                    rule, column_name
                )))
            }
        }
    }
    Ok((domain_rules, stats_rules, unicity, null_rule))
}

/// Compile a column builder into an executable column.
///
/// This is the main entry point for rule compilation. Takes a `ColumnBuilder` and produces
/// an `ExecutableColumn` with type-specific validators ready for runtime execution.
///
/// For CSV tables, a `TypeCheck` is always added to handle string-to-type conversion.
/// Parquet skip type checking as their types are native.
///
/// # Errors
///
/// Returns `RuleError::ValidationError` if:
/// - A rule doesn't match the column type (e.g., StringLength on an Integer column)
/// - Invalid regex pattern in StringRegex rule
pub fn compile_column(
    builder: Box<dyn ColumnBuilder>,
    need_type_check: bool,
) -> Result<ExecutableColumn, RuleError> {
    match builder.column_type() {
        ColumnType::String => {
            let (executable_rules, unicity_check, null_check) =
                compile_string_rules(builder.rules(), builder.name())?;
            let mut type_check = None;
            if need_type_check {
                let t = builder.type_threshold();
                type_check = Some(TypeCheck::new(
                    builder.name().to_string(),
                    DataType::Utf8,
                    t,
                ));
            }
            Ok(ExecutableColumn::String {
                name: builder.name().to_string(),
                rules: executable_rules,
                type_check,
                unicity_check,
                null_check,
            })
        }
        ColumnType::Integer => {
            let (domain_rules, statistical_rules, unicity_check, null_check) =
                compile_numeric_rules(builder.rules(), builder.name())?;
            let mut type_check = None;
            if need_type_check {
                let t = builder.type_threshold();
                type_check = Some(TypeCheck::new(
                    builder.name().to_string(),
                    DataType::Int64,
                    t,
                ));
            }
            Ok(ExecutableColumn::Integer {
                name: builder.name().to_string(),
                domain_rules,
                statistical_rules,
                type_check,
                unicity_check,
                null_check,
            })
        }
        ColumnType::Float => {
            let (executable_rules, statistical_rules, unicity_check, null_check) =
                compile_numeric_rules(builder.rules(), builder.name())?;
            let mut type_check = None;
            if need_type_check {
                let t = builder.type_threshold();
                type_check = Some(TypeCheck::new(
                    builder.name().to_string(),
                    DataType::Float64,
                    t,
                ));
            }
            Ok(ExecutableColumn::Float {
                name: builder.name().to_string(),
                domain_rules: executable_rules,
                statistical_rules,
                type_check,
                unicity_check,
                null_check,
            })
        }
        ColumnType::DateType => {
            let (executable_rules, unicity_check, null_check) =
                compile_date_rules(builder.rules(), builder.name())?;
            let mut type_check = None;
            if need_type_check {
                // Safety: DateColumnBuilder can only return Some()
                let format = builder.format().unwrap();
                let t = builder.type_threshold();
                type_check = Some(DateTypeCheck::new(
                    builder.name().to_string(),
                    DataType::Date32,
                    format.to_string(),
                    t,
                ));
            }
            Ok(ExecutableColumn::Date {
                name: builder.name().to_string(),
                rules: executable_rules,
                type_check,
                unicity_check,
                null_check,
            })
        }
    }
}

/// Build a map of column names to their Arrow DataTypes.
///
/// This helper function extracts type information from column builders,
/// which is used by relation compilation to determine the correct Arrow type
/// for numeric comparisons.
pub fn build_column_type_map(columns: &[Box<dyn ColumnBuilder>]) -> HashMap<String, DataType> {
    columns
        .iter()
        .map(|col| {
            let data_type = match col.column_type() {
                ColumnType::Integer => DataType::Int64,
                ColumnType::Float => DataType::Float64,
                ColumnType::String => DataType::Utf8,
                ColumnType::DateType => DataType::Date32,
            };
            (col.name().to_string(), data_type)
        })
        .collect()
}

pub fn compile_relations(
    builder: RelationBuilder,
    column_types: &HashMap<String, DataType>,
) -> Result<ExecutableRelation, RuleError> {
    let RelationBuilder { names, rules } = builder;
    let mut executable_relations: Vec<Box<dyn RelationRule>> = Vec::new();

    // For the cli we ensure from the config that the Column is present, we do not have the same
    // check in api, so we need to do this here
    let left_type = column_types.get(&names[0]).ok_or_else(|| {
        RuleError::ValidationError(format!("Column '{}' not found in relation", names[0]))
    })?;
    let right_type = column_types.get(&names[1]).ok_or_else(|| {
        RuleError::ValidationError(format!("Column '{}' not found in relation", names[1]))
    })?;

    // We only validate columns of same type
    if left_type != right_type {
        return Err(RuleError::ValidationError(format!(
            "Cannot compare columns of different types: '{}' ({:?}) vs '{}' ({:?})",
            names[0], left_type, names[1], right_type
        )));
    }

    for rule in rules {
        match rule {
            TableConstraint::DateComparaison { op, threshold } => match left_type {
                DataType::Date32 => {
                    executable_relations
                        .push(Box::new(CompareCheck::<Date32Type>::new(op, threshold)));
                }
                other_type => {
                    return Err(RuleError::ValidationError(format!(
                        "Date comparison not supported for type {:?}. Only Date32 is supported.",
                        other_type
                    )));
                }
            },
            TableConstraint::NumericComparaison { op, threshold } => {
                // Create appropriate CompareCheck based on the type
                match left_type {
                    DataType::Int64 => {
                        executable_relations
                            .push(Box::new(CompareCheck::<Int64Type>::new(op, threshold)));
                    }
                    DataType::Float64 => {
                        executable_relations
                            .push(Box::new(CompareCheck::<Float64Type>::new(op, threshold)));
                    }
                    other_type => {
                        return Err(RuleError::ValidationError(
                            format!(
                                "Numeric comparison not supported for type {:?}. Only Int64 and Float64 are supported.",
                                other_type
                            )
                        ));
                    }
                }
            }
        }
    }
    Ok(ExecutableRelation::new(names, executable_relations))
}
