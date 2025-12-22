use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crate::{types::UnicityRecord, utils::hasher::Xxh3Builder, validator::ExecutableColumn};

/// Manages uniqueness checking across batches.
///
/// Collects hashes during parallel validation, then calculates
/// duplicate counts after all batches are processed.
pub(crate) struct UnicityAccumulator {
    // Column name → global hash set (thread-safe)
    accumulators: HashMap<String, UnicityRecord>,
}

impl UnicityAccumulator {
    /// Create accumulator for columns that have unicity checks.
    pub fn new(columns: &[ExecutableColumn]) -> Self {
        let mut accumulators: HashMap<String, UnicityRecord> = HashMap::new();

        for column in columns {
            if column.has_unicity() {
                let map = Arc::new(Mutex::new(HashSet::with_capacity_and_hasher(
                    2_000_000,
                    Xxh3Builder,
                )));
                let null_counter = AtomicUsize::new(0);
                accumulators.insert(column.get_name(), (null_counter, map));
            }
        }

        Self { accumulators }
    }

    /// Record hashes from a batch for a specific column.
    ///
    /// # Panics
    ///
    /// Panics if `column_name` was not registered during `new()`.
    /// This indicates a programming error in the validation engine.
    pub fn record_hashes(
        &self,
        column_name: &str,
        null_count: usize,
        hashes: HashSet<u64, Xxh3Builder>,
    ) {
        // TODO: for now we unwrap column_name should always be set
        let (counter, map) = self.accumulators.get(column_name).unwrap();
        counter.fetch_add(null_count, Ordering::Relaxed);
        map.lock().unwrap().extend(hashes)
    }

    /// Calculate error counts for all columns.
    /// Returns: HashMap<column_name, error_count>
    pub fn finalize(&self, total_rows: usize) -> HashMap<String, usize> {
        self.accumulators
            .iter()
            .map(|(name, (c, h))| {
                let u = h.lock().unwrap().len();
                let n = c.load(Ordering::Relaxed);
                // We get the total number of rows
                // We substract the null count, to get the total valid row
                // Than we compare both len to get the not unique value
                let r = total_rows - n - u;
                (name.to_owned(), r)
            })
            .collect::<HashMap<String, usize>>()
    }
}
