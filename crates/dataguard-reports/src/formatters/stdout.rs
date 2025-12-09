use dataguard_core::ValidationResult;

use crate::utils::numbers::format_numbers;

pub struct StdOutFormatter {}

impl StdOutFormatter {
    pub fn new(version: String) -> Self {
        let s = format!("DataGuard v{} - Validation Report", version);
        let n = s.len();
        let i = "=".repeat(n);

        println!("{}", s);
        println!("{}", i);
        Self {}
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
}
