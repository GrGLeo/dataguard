use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::{utils::hasher::Xxh3Builder, validator::ExecutableColumn};

/// Manages uniqueness checking across batches.
///
/// Collects hashes during parallel validation, then calculates
/// duplicate counts after all batches are processed.
pub(crate) struct UnicityAccumulator {
    // Column name → global hash set (thread-safe)
    accumulators: HashMap<String, Arc<Mutex<HashSet<u64, Xxh3Builder>>>>,
}

impl UnicityAccumulator {
    /// Create accumulator for columns that have unicity checks.
    pub fn new(columns: &[ExecutableColumn]) -> Self {
        let mut accumulators: HashMap<String, Arc<Mutex<HashSet<u64, Xxh3Builder>>>> =
            HashMap::new();

        for column in columns {
            if column.has_unicity() {
                accumulators.insert(
                    column.get_name(),
                    Arc::new(Mutex::new(HashSet::with_hasher(Xxh3Builder))),
                );
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
    pub fn record_hashes(&self, column_name: &str, hashes: HashSet<u64, Xxh3Builder>) {
        // TODO: for now we unwrap column_name should always be set
        let mut hash = self.accumulators.get(column_name).unwrap().lock().unwrap();
        hash.extend(hashes)
    }

    /// Calculate error counts for all columns.
    /// Returns: HashMap<column_name, error_count>
    pub fn finalize(&self, total_rows: usize) -> HashMap<String, usize> {
        self.accumulators
            .iter()
            .map(|(name, h)| {
                let u = h.lock().unwrap().len();
                let r = total_rows - u;
                (name.to_owned(), r)
            })
            .collect::<HashMap<String, usize>>()
    }
}
