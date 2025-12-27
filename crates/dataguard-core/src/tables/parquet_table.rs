use std::collections::HashMap;

use crate::{
    columns::{relation_builder::RelationBuilder, ColumnBuilder},
    compiler, engine,
    readers::read_parquet_parallel,
    validator::{ExecutableColumn, ExecutableRelation},
    RuleError, Table, ValidationResult,
};

pub struct ParquetTable {
    path: String,
    table_name: String,
    executable_columns: Box<[ExecutableColumn]>,
    executable_relations: Option<Box<[ExecutableRelation]>>,
}

impl ParquetTable {
    /// Create a new Validator instance
    pub fn new(path: String, table_name: String) -> Result<Self, RuleError> {
        Ok(Self {
            path,
            table_name,
            executable_columns: Box::new([]),
            executable_relations: None,
        })
    }
}

impl Table for ParquetTable {
    /// Commit column configurations and compile them into executable rules
    fn prepare(
        &mut self,
        columns: Vec<Box<dyn ColumnBuilder>>,
        relations: Vec<RelationBuilder>,
    ) -> Result<(), RuleError> {
        // Here with parquet, we do not add TypeCheck, we use the schema from the parquet file
        // Since we only support a small range of available Arrow type for now this is incomplete
        self.executable_columns = columns
            .into_iter()
            .map(|col| compiler::compile_column(col, false))
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();

        self.executable_relations = Some(
            relations
                .into_iter()
                .map(compiler::compile_relations)
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice(),
        );
        Ok(())
    }

    /// Validate a Parquet file against the committed rules
    fn validate(&mut self) -> Result<ValidationResult, RuleError> {
        let needed_cols: Vec<String> = self
            .executable_columns
            .iter()
            .map(|v| v.get_name())
            .collect();
        let batches = read_parquet_parallel(self.path.as_str(), needed_cols)?;
        let engine =
            engine::ValidationEngine::new(&self.executable_columns, &self.executable_relations);
        engine.validate_batches(self.table_name.clone(), &batches)
    }

    /// Get a summary of configured rules
    fn get_rules(&self) -> HashMap<String, Vec<String>> {
        let mut result = HashMap::new();
        for column in &self.executable_columns {
            match column {
                ExecutableColumn::String { name, rules, .. } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
                ExecutableColumn::Integer {
                    name,
                    domain_rules,
                    statistical_rules,
                    ..
                } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(domain_rules.iter().map(|r| r.name().to_string()));
                    rule_names.extend(statistical_rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
                ExecutableColumn::Float {
                    name,
                    domain_rules: rules,
                    statistical_rules,
                    ..
                } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    rule_names.extend(statistical_rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
                ExecutableColumn::Date { name, rules, .. } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
            }
        }
        result
    }
}
