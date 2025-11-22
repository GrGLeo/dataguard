use dashmap::DashMap;
use prettytable::{Cell, Row, Table};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ValidationReport {
    results: DashMap<(String, String), AtomicUsize>, // (column_name, rule_name) -> error_count
    total_rows: AtomicUsize,
}

impl ValidationReport {
    pub fn new() -> Self {
        Self {
            results: DashMap::new(),
            total_rows: AtomicUsize::new(0),
        }
    }

    pub fn record_result(&self, column_name: &str, rule_name: &str, error_count: usize) {
        self.results
            .entry((column_name.to_string(), rule_name.to_string()))
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(error_count, Ordering::Relaxed);
    }

    pub fn set_total_rows(&self, total_rows: usize) {
        self.total_rows.store(total_rows, Ordering::Relaxed);
    }

    pub fn generate_report(&self) -> String {
        let mut table = Table::new();
        table.add_row(Row::new(vec![
            Cell::new("Column"),
            Cell::new("Rule"),
            Cell::new("Error Count"),
            Cell::new("% Error"),
        ]));

        let total_rows = self.total_rows.load(Ordering::Relaxed);

        // Sort results for consistent output
        let mut sorted_results: Vec<_> = self.results.iter().collect();
        sorted_results.sort_by(|a, b| {
            let col_cmp = a.key().0.cmp(&b.key().0);
            if col_cmp != std::cmp::Ordering::Equal {
                col_cmp
            } else {
                a.key().1.cmp(&b.key().1)
            }
        });

        for entry in sorted_results {
            let (column_name, rule_name) = entry.key();
            let error_count = entry.value().load(Ordering::Relaxed);
            let error_percentage = if total_rows > 0 {
                (error_count as f64 / total_rows as f64) * 100.0
            } else {
                0.0
            };

            table.add_row(Row::new(vec![
                Cell::new(column_name),
                Cell::new(rule_name),
                Cell::new(&error_count.to_string()),
                Cell::new(&format!("{:.2}%", error_percentage)),
            ]));
        }

        table.to_string()
    }
}
