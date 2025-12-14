use chrono::Local;
use serde::{Deserialize, Serialize};
use serde_json::Error;

use crate::Reporter;

#[derive(Serialize, Deserialize)]
pub struct JsonFormatter {
    version: String,
    timestamp: String,
    tables: Vec<TableFormatter>,
}

#[derive(Serialize, Deserialize)]
struct TableFormatter {
    name: String,
    n_rows: usize,
    pass: bool,
    columns: Vec<ColumnFomatter>,
}

#[derive(Serialize, Deserialize)]
struct ColumnFomatter {
    name: String,
    rules: Vec<RuleFormatter>,
}

#[derive(Serialize, Deserialize)]
struct RuleFormatter {
    name: String,
    errors: usize,
    error_percent: f64,
}

impl JsonFormatter {
    pub fn new(version: String) -> Self {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        Self {
            version,
            timestamp,
            tables: Vec::new(),
        }
    }

    pub fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string_pretty(self)
    }
}

impl Reporter for JsonFormatter {
    fn on_start(&self) {}

    fn on_loading(&self) {}

    fn on_table_load(&self, _current: usize, _total: usize, _name: &str) {}

    fn on_validation_start(&self) {}

    fn on_table_result(&mut self, result: &dataguard_core::ValidationResult) {
        let name = result.table_name.clone();
        let n_rows = result.total_rows;
        let columns: Vec<ColumnFomatter> = result
            .get_column_results()
            .into_iter()
            .map(|(n, c)| {
                let rules: Vec<RuleFormatter> = c
                    .into_iter()
                    .map(|r| RuleFormatter {
                        name: r.rule_name.clone(),
                        errors: r.error_count,
                        error_percent: r.error_percentage,
                    })
                    .collect();
                ColumnFomatter { name: n, rules }
            })
            .collect();
        let table = TableFormatter {
            name,
            n_rows,
            columns,
            pass: result.is_passed(),
        };
        self.tables.push(table);
    }

    fn on_summary(&self, _passed: usize, _failed: usize) {}

    fn on_waiting(&self) {}
}
