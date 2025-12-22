use std::collections::HashMap;

use crate::{
    columns::{relation_builder::RelationBuilder, ColumnBuilder},
    RuleError, ValidationResult,
};

pub mod csv_table;

pub trait Table {
    fn validate(&mut self) -> Result<ValidationResult, RuleError>;
    fn prepare(
        &mut self,
        columns: Vec<Box<dyn ColumnBuilder>>,
        relations: Vec<RelationBuilder>,
    ) -> Result<(), RuleError>;
    fn get_rules(&self) -> HashMap<String, Vec<String>>;
}
