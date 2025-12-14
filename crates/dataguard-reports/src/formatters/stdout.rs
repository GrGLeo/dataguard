use dataguard_core::ValidationResult;

use crate::{utils::numbers::format_numbers, Reporter};

pub struct StdOutFormatter {
    intro: String,
    intro_len: usize,
}

impl StdOutFormatter {
    pub fn new(version: String) -> Self {
        let s = format!("DataGuard v{} - Validation Report", version);
        let n = s.len();
        Self {
            intro: s,
            intro_len: n,
        }
    }
    pub fn print_loading_start(&self) {
        println!("Loading data...");
    }

    pub fn print_loading_progress(&self, current: usize, total: usize, name: &str) {
        println!("  [{}/{}] {}", current, total, name);
    }

    pub fn print_validation_start(&self) {
        println!("\nValidating...");
    }

    pub fn print_table_result(&self, result: &ValidationResult) {
        let status = if result.is_passed() {
            "PASSED"
        } else {
            "FAILED"
        };
        let rows_formatted = format_numbers(result.total_rows);

        println!(
            "\n{} ({} rows) - {}",
            result.table_name, rows_formatted, status
        );

        for (column_name, rule_results) in result.get_column_results() {
            println!("  {}:", column_name);

            let max_len = rule_results
                .iter()
                .map(|r| r.rule_name.len())
                .max()
                .unwrap_or(0);

            for rule in rule_results {
                let dots = ".".repeat(max_len - rule.rule_name.len() + 10);
                let count_str = format_numbers(rule.error_count);
                println!(
                    "    {} {} {:>6} ({:.2}%)",
                    rule.rule_name, dots, count_str, rule.error_percentage
                );
            }
        }

        if let Some(error_msg) = &result.error_message {
            println!("  Error: {}", error_msg);
        }
    }

    pub fn print_summary(&self, passed: usize, failed: usize) {
        println!("\n===================================");
        println!("Result: {} failed, {} passed", failed, passed);
    }

    pub fn print_waiting(&self) {
        let i = "=".repeat(self.intro_len);

        println!("\n{}", i);
        println!("Waiting for file changes...");
    }
}

impl Reporter for StdOutFormatter {
    fn on_start(&self) {
        let i = "=".repeat(self.intro_len);

        println!("{}", self.intro);
        println!("{}", i);
    }

    fn on_loading(&self) {
        self.print_loading_start();
    }

    fn on_table_load(&self, current: usize, total: usize, name: &str) {
        self.print_loading_progress(current, total, name);
    }

    fn on_validation_start(&self) {
        self.print_validation_start();
    }

    fn on_table_result(&mut self, result: &ValidationResult) {
        self.print_table_result(result);
    }

    fn on_summary(&self, passed: usize, failed: usize) {
        self.print_summary(passed, failed);
    }

    fn on_waiting(&self) {
        self.print_waiting();
    }
}
