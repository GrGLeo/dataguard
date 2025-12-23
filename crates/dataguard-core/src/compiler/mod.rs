//! Rule compilation module.
//!
//! Converts high-level `ColumnBuilder` with `ColumnRule` enums into executable
//! `ExecutableColumn` with trait objects for runtime validation.
//!
//! This module is used by table implementations (CsvTable, future ParquetTable, etc.)
//! to compile user-defined validation rules into optimized, type-specific validators.

use std::fmt::Debug;

use arrow::datatypes::DataType;
use arrow_array::ArrowNumericType;
use num_traits::{Num, NumCast};

#[cfg(test)]
mod tests;

use crate::{
    columns::{relation_builder::RelationBuilder, ColumnBuilder, TableConstraint},
    rules::{
        date::{DateBoundaryCheck, DateRule, DateTypeCheck},
        relations::{DateCompareCheck, RelationRule},
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
            ColumnRule::StringLength { name, min, max } => {
                executable_rules.push(Box::new(StringLengthCheck::new(name.clone(), *min, *max)));
            }
            ColumnRule::StringRegex {
                name,
                pattern,
                flags,
            } => {
                executable_rules.push(Box::new(RegexMatch::new(
                    name.clone(),
                    pattern.clone(),
                    flags.clone(),
                )));
            }
            ColumnRule::StringMembers { name, members } => {
                executable_rules.push(Box::new(IsInCheck::new(name.clone(), members.to_vec())));
            }
            ColumnRule::Unicity => {
                unicity_check = Some(UnicityCheck::new());
            }
            ColumnRule::NullCheck => {
                null_check = Some(NullCheck::new());
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
            ColumnRule::Unicity => {
                unicity_check = Some(UnicityCheck::new());
            }
            ColumnRule::NullCheck => {
                null_check = Some(NullCheck::new());
            }
            ColumnRule::DateBoundary {
                name,
                after,
                year,
                month,
                day,
            } => {
                let rule = DateBoundaryCheck::new(name.clone(), *after, *year, *month, *day)?;
                executable_rules.push(Box::new(rule));
            }
            ColumnRule::WeekDay { name, is_week } => {
                let rule = WeekDayCheck::new(name.clone(), *is_week);
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
        Vec<Box<dyn NumericRule<A>>>,
        Option<UnicityCheck>,
        Option<NullCheck>,
    ),
    RuleError,
>
where
    N: Num + PartialOrd + Copy + Debug + Send + Sync + NumCast + 'static,
    A: ArrowNumericType<Native = N>,
{
    let mut unicity = None;
    let mut null_rule = None;
    let mut executable_rules: Vec<Box<dyn NumericRule<A>>> = Vec::new();
    for rule in rules {
        match rule {
            ColumnRule::NumericRange { name, min, max } => {
                let min_conv = min.and_then(|v| N::from(v));
                let max_conv = max.and_then(|v| N::from(v));
                executable_rules.push(Box::new(Range::<N>::new(name.clone(), min_conv, max_conv)));
            }
            ColumnRule::Monotonicity { name, ascending } => {
                executable_rules.push(Box::new(Monotonicity::<N>::new(name.clone(), *ascending)));
            }
            ColumnRule::NullCheck => null_rule = Some(NullCheck::new()),
            ColumnRule::Unicity => {
                unicity = Some(UnicityCheck::new());
            }
            _ => {
                return Err(RuleError::ValidationError(format!(
                    "Invalid rule {:?} for numeric column '{}'",
                    rule, column_name
                )))
            }
        }
    }
    Ok((executable_rules, unicity, null_rule))
}

/// Compile a column builder into an executable column.
///
/// This is the main entry point for rule compilation. Takes a `ColumnBuilder` and produces
/// an `ExecutableColumn` with type-specific validators ready for runtime execution.
///
/// For CSV tables, a `TypeCheck` is always added to handle string-to-type conversion.
/// Future table types (Parquet, SQL) may skip type checking as their types are native.
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
                type_check = Some(TypeCheck::new(builder.name().to_string(), DataType::Utf8));
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
            let (executable_rules, unicity_check, null_check) =
                compile_numeric_rules(builder.rules(), builder.name())?;
            let mut type_check = None;
            if need_type_check {
                type_check = Some(TypeCheck::new(builder.name().to_string(), DataType::Int64));
            }
            Ok(ExecutableColumn::Integer {
                name: builder.name().to_string(),
                rules: executable_rules,
                type_check,
                unicity_check,
                null_check,
            })
        }
        ColumnType::Float => {
            let (executable_rules, unicity_check, null_check) =
                compile_numeric_rules(builder.rules(), builder.name())?;
            let mut type_check = None;
            if need_type_check {
                type_check = Some(TypeCheck::new(
                    builder.name().to_string(),
                    DataType::Float64,
                ));
            }
            Ok(ExecutableColumn::Float {
                name: builder.name().to_string(),
                rules: executable_rules,
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
                type_check = Some(DateTypeCheck::new(
                    builder.name().to_string(),
                    DataType::Date32,
                    format.to_string(),
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

pub fn compile_relations(builder: RelationBuilder) -> Result<ExecutableRelation, RuleError> {
    let RelationBuilder { names, rules } = builder;
    let mut executable_relations: Vec<Box<dyn RelationRule>> = Vec::new();
    for rule in rules {
        match rule {
            TableConstraint::DateComparaison { op } => {
                executable_relations.push(Box::new(DateCompareCheck::new(op)));
            }
        }
    }
    Ok(ExecutableRelation::new(names, executable_relations))
}
