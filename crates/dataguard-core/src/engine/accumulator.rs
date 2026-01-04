use dashmap::DashMap;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use crate::{
    types::{RuleResultMap, ValidationMapReport},
    RuleResult,
};

/// Thread-safe accumulator for validation errors.
///
/// Collects error counts per (column, rule) pair during parallel validation.
/// After all batches are processed, converts to structured results with percentages.
pub struct ResultAccumulator {
    // (column_name, rule_name) -> error_count
    column_results: DashMap<(String, String), (AtomicUsize, Mutex<f64>)>,
    // (column_name, rule_name) -> error_count
    relation_results: DashMap<(String, String), (AtomicUsize, Mutex<f64>)>,
    // column_name -> total_valid_values
    valid_values: DashMap<String, AtomicUsize>,
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
            column_results: DashMap::new(),
            relation_results: DashMap::new(),
            valid_values: DashMap::new(),
            total_rows: AtomicUsize::new(0),
        }
    }

    /// Set the total number of rows being validated.
    ///
    /// Must be called before `to_results()` to get correct percentages.
    pub fn set_total_rows(&self, total_rows: usize) {
        self.total_rows.store(total_rows, Ordering::Relaxed);
    }

    /// Record total valid value per column
    /// We call valid values all initial non null values
    ///
    /// Thread-safe - can be called from multiple threads concurrently.
    pub fn record_valid_values(&self, column_name: &str, array_values: usize) {
        self.valid_values
            .entry(column_name.to_string())
            .and_modify(|total| {
                total.fetch_add(array_values, Ordering::Relaxed);
            })
            .or_insert_with(|| AtomicUsize::new(array_values));
    }

    /// Record errors for a specific column and rule.
    ///
    /// Thread-safe - can be called from multiple threads concurrently.
    pub fn record_column_result(
        &self,
        column_name: &str,
        rule_name: String,
        threshold: f64,
        error_count: usize,
    ) {
        self.column_results
            .entry((column_name.to_string(), rule_name))
            .and_modify(|(counter, _)| {
                counter.fetch_add(error_count, Ordering::Relaxed);
            })
            .or_insert_with(|| (AtomicUsize::new(error_count), Mutex::new(threshold)));
    }

    /// Record errors for a specific relation and rule.
    ///
    /// Thread-safe - can be called from multiple threads concurrently.
    pub fn record_relation_result(
        &self,
        relation_name: &str,
        rule_name: String,
        threshold: f64,
        error_count: usize,
    ) {
        self.relation_results
            .entry((relation_name.to_string(), rule_name))
            .and_modify(|(counter, _)| {
                counter.fetch_add(error_count, Ordering::Relaxed);
            })
            .or_insert_with(|| (AtomicUsize::new(0), Mutex::new(threshold)));
    }

    /// Consolidates atomic counters into a final report of validation results.
    ///
    /// This method performs the following:
    /// - Snapshots current atomic values for valid row counts.
    /// - Iterates through column and relation rule failures.
    /// - Calculates error percentages based on `total_rows`.
    /// - Groups results by their respective column or relation names.
    ///
    /// Returns a `ValidationMapReport` type containing all mapped data.
    pub fn to_results(&self) -> ValidationMapReport {
        let mut column_results: RuleResultMap = HashMap::new();
        let mut relation_results: RuleResultMap = HashMap::new();
        let total_rows = self.total_rows.load(Ordering::Relaxed);

        let column_values = self
            .valid_values
            .iter()
            .map(|entry| {
                let (k, v) = entry.pair();
                (k.clone(), v.load(Ordering::Relaxed))
            })
            .collect::<HashMap<String, usize>>();

        let mut sorted: Vec<_> = self.column_results.iter().collect();
        sorted.sort_by(|a, b| a.key().0.cmp(&b.key().0));

        for entry in sorted {
            let mut error_message = None;
            let (column_name, rule_name) = entry.key();
            let valid_values = self
                .valid_values
                .get(column_name)
                .unwrap()
                .value()
                .load(Ordering::Relaxed);
            let error_count = entry.value().0.load(Ordering::Relaxed);
            let threshold = entry.value().1.lock().unwrap().to_owned();
            let error_percentage = if total_rows > 0 {
                (error_count as f64 / total_rows as f64) * 100.
            } else {
                0.0
            };
            if error_count == valid_values {
                error_message = Some(String::from(
                    r"/!\ TypeCast failure all associated rules are passed",
                ));
            }

            column_results
                .entry(column_name.clone())
                .or_default()
                .push(RuleResult::new(
                    rule_name.clone(),
                    error_count,
                    error_percentage,
                    threshold,
                    error_message,
                    error_percentage <= threshold,
                ));
        }

        let mut sorted: Vec<_> = self.relation_results.iter().collect();
        sorted.sort_by(|a, b| a.key().0.cmp(&b.key().0));

        for entry in sorted {
            let (relation_name, rule_name) = entry.key();
            let error_count = entry.value().0.load(Ordering::Relaxed);
            let threshold = entry.value().1.lock().unwrap().to_owned();
            let error_percentage = if total_rows > 0 {
                (error_count as f64 / total_rows as f64) * 100.
            } else {
                0.0
            };

            relation_results
                .entry(relation_name.clone())
                .or_default()
                .push(RuleResult::new(
                    rule_name.clone(),
                    error_count,
                    error_percentage,
                    threshold,
                    None,
                    error_percentage <= threshold,
                ));
        }

        (column_values, column_results, relation_results)
    }
}
