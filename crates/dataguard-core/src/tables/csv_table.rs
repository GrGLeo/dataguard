use std::collections::HashMap;
use std::fs::File;

use crate::columns::{relation_builder::RelationBuilder, ColumnBuilder};
use crate::errors::RuleError;
use crate::readers::{read_parallel, read_streaming, FileFormat, ReaderConfig};
use crate::tables::Table;
use crate::validator::{ExecutableColumn, ExecutableRelation};
use crate::{compiler, engine, ValidationResult};

pub struct CsvTable {
    path: String,
    table_name: String,
    executable_columns: Box<[ExecutableColumn]>,
    executable_relations: Option<Box<[ExecutableRelation]>>,
}

impl CsvTable {
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

impl Table for CsvTable {
    /// Commit column configurations and compile them into executable rules
    fn prepare(
        &mut self,
        columns: Vec<Box<dyn ColumnBuilder>>,
        relations: Vec<RelationBuilder>,
    ) -> Result<(), RuleError> {
        self.executable_columns = columns
            .into_iter()
            .map(|col| compiler::compile_column(col, true))
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

    /// Validate a CSV file against the committed rules
    fn validate(&mut self) -> Result<ValidationResult, RuleError> {
        let needed_cols: Vec<String> = self
            .executable_columns
            .iter()
            .map(|v| v.get_name())
            .collect();

        let config = ReaderConfig::default();

        // Check file size to determine streaming vs batch mode
        let file = File::open(&self.path)?;
        let file_size = file.metadata()?.len();
        drop(file);

        let engine =
            engine::ValidationEngine::new(&self.executable_columns, &self.executable_relations);

        if config.should_stream(file_size) {
            let receiver =
                read_streaming(self.path.as_str(), needed_cols, FileFormat::Csv, config)?;
            // We validate batch by batch
            engine.validate_batches_streaming(self.table_name.clone(), receiver)
        } else {
            // We first load the file in memory as Arrow batches
            let batches = read_parallel(self.path.as_str(), needed_cols, FileFormat::Csv, &config)?;
            engine.validate_batches(self.table_name.clone(), &batches)
        }
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
                    domain_rules: rules,
                    statistical_rules,
                    ..
                } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
                    rule_names.extend(statistical_rules.iter().map(|r| r.name().to_string()));
                    result.insert(name.clone(), rule_names);
                }
                ExecutableColumn::Float {
                    name,
                    domain_rules: rules,
                    ..
                } => {
                    let mut rule_names = vec!["TypeCheck".to_string()];
                    rule_names.extend(rules.iter().map(|r| r.name().to_string()));
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
