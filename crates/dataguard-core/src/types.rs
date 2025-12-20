use std::{collections::HashSet, sync::{atomic::AtomicUsize, Arc, Mutex}};

use crate::utils::hasher::Xxh3Builder;

pub type Batch = arrow::record_batch::RecordBatch;
pub type Batches = Vec<Batch>;
pub type UnicityRecord = (AtomicUsize, Arc<Mutex<HashSet<u64, Xxh3Builder>>>);
