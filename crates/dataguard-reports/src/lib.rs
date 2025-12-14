pub mod formatters;
pub mod utils;

use dataguard_core::ValidationResult;
pub use formatters::{stdout::StdOutFormatter, json::JsonFormatter};

pub trait Reporter {
    fn on_start(&self);
    fn on_loading(&self);
    fn on_table_load(&self, current: usize, total: usize, name: &str);
    fn on_validation_start(&self);
    fn on_table_result(&mut self, result: &ValidationResult);
    fn on_summary(&self, passed: usize, failed: usize);
    fn on_waiting(&self);
}
