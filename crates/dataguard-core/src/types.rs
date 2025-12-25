use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicUsize, Arc, Mutex},
};

use crate::{utils::hasher::Xxh3Builder, RuleResult};

pub type Batch = arrow::record_batch::RecordBatch;
pub type Batches = Vec<Batch>;
pub type UnicityRecord = (AtomicUsize, Arc<Mutex<HashSet<u64, Xxh3Builder>>>, f64);

/// Maps column names to their valid row counts
pub type ValidValueMap = HashMap<String, usize>;

/// Maps names (column or relation) to their associated rule execution results
pub type RuleResultMap = HashMap<String, Vec<RuleResult>>;

/// The complete report generated after validation
pub type ValidationMapReport = (ValidValueMap, RuleResultMap, RuleResultMap);
