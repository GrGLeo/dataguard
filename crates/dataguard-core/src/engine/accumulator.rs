//! Result accumulation during validation.
//!
//! `ResultAccumulator` provides thread-safe collection of validation errors
//! during parallel batch processing.
use dashmap::DashMap;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::RuleResult;

/// Thread-safe accumulator for validation errors.
///
/// Collects error counts per (column, rule) pair during parallel validation.
/// After all batches are processed, converts to structured results with percentages.
pub struct ResultAccumulator {
    results: DashMap<(String, String), AtomicUsize>, // (column_name, rule_name) -> error_count
    total_rows: AtomicUsize,
}

impl Default for ResultAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultAccumulator {
    /// Create a new empty accumulator.
    pub fn new() -> Self {
        Self {
            results: DashMap::new(),
            total_rows: AtomicUsize::new(0),
        }
    }

    /// Set the total number of rows being validated.
    ///
    /// Must be called before `to_results()` to get correct percentages.
    pub fn set_total_rows(&self, total_rows: usize) {
        self.total_rows.store(total_rows, Ordering::Relaxed);
    }

    /// Record errors for a specific column and rule.
    ///
    /// Thread-safe - can be called from multiple threads concurrently.
    pub fn record_result(&self, column_name: &str, rule_name: &str, error_count: usize) {
        self.results
            .entry((column_name.to_string(), rule_name.to_string()))
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(error_count, Ordering::Relaxed);
    }

    /// Convert accumulated results to structured format.
    ///
    /// Returns a map of column names to their rule results, sorted by column then rule name.
    /// Error percentages are calculated based on `total_rows`.
    pub fn to_results(&self) -> HashMap<String, Vec<RuleResult>> {
        let mut results: HashMap<String, Vec<RuleResult>> = HashMap::new();
        let total_rows = self.total_rows.load(Ordering::Relaxed);

        let mut sorted: Vec<_> = self.results.iter().collect();
        sorted.sort_by(|a, b| {
            let col_cmp = a.key().0.cmp(&b.key().0);
            if col_cmp != std::cmp::Ordering::Equal {
                col_cmp
            } else {
                a.key().1.cmp(&b.key().1)
            }
        });

        for entry in sorted {
            let (column_name, rule_name) = entry.key();
            let error_count = entry.value().load(Ordering::Relaxed);
            let error_percentage = if total_rows > 0 {
                (error_count as f64 / total_rows as f64) * 100.
            } else {
                0.0
            };

            results
                .entry(column_name.clone())
                .or_default()
                .push(RuleResult::new(
                    rule_name.clone(),
                    error_count,
                    error_percentage,
                ));
        }

        results
    }
}
