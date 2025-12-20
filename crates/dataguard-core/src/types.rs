use std::sync::atomic::AtomicUsize;

use dashmap::DashSet;

use crate::utils::hasher::Xxh3Builder;

pub type Batch = arrow::record_batch::RecordBatch;
pub type Batches = Vec<Batch>;
pub type UnicityRecord = (AtomicUsize, DashSet<u64, Xxh3Builder>);
