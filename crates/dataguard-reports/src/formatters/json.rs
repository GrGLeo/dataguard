use chrono::Local;
use serde::{Deserialize, Serialize};
use serde_json::Error;

use crate::Reporter;

#[derive(Serialize, Deserialize)]
pub struct JsonFormatter {
    version: String,
    timestamp: String,
    #[serde(skip)]
    timestamp_compact: String,
    #[serde(skip)]
    brief: bool,
    tables: Vec<TableFormatter>,
}

#[derive(Serialize, Deserialize)]
struct TableFormatter {
    name: String,
    n_rows: usize,
    pass: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<ColumnFomatter>>,
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
    pub fn new(version: String, brief: bool) -> Self {
        let now = Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S").to_string();
        let timestamp_compact = now.format("%Y%m%d-%H%M%S").to_string();
        Self {
            version,
            timestamp,
            timestamp_compact,
            brief,
            tables: Vec::new(),
        }
    }

    pub fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn get_timestamp_compact(&self) -> &str {
        &self.timestamp_compact
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

        // Only include columns if not in brief mode
        let columns = if self.brief {
            None
        } else {
            Some(
                result
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
                    .collect(),
            )
        };

        let table = TableFormatter {
            name,
            n_rows,
            columns,
            pass: result.is_passed(),
        };
        self.tables.push(table);
    }

    fn on_complete(&self, _passed: usize, _failed: usize) {}

    fn on_waiting(&self) {}
}
