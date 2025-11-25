extern crate dataguard;

use arrow::array::Int64Array;
use criterion::{Criterion, criterion_group, criterion_main};
use dataguard::rules::logic::{IntegerRule, Monotonicity};
use once_cell::sync::Lazy;
use std::hint::black_box;
use std::sync::Arc;

// Helper function to create an Int64Array with a given size.
// It creates a monotonically increasing sequence.
fn create_int_array(size: usize) -> Int64Array {
    let values: Vec<i64> = (0..size as i64).collect();
    Int64Array::from(values)
}

// Prebuild arrays once and reuse across all benchmark functions.
static PREBUILT_ARRAYS: Lazy<Vec<(usize, Arc<Int64Array>)>> = Lazy::new(|| {
    let sizes = [1_000usize, 10_000, 100_000, 300_000];
    let mut v = Vec::with_capacity(sizes.len());
    for &size in sizes.iter() {
        let arr = Arc::new(create_int_array(size));
        v.push((size, arr));
    }
    v
});

// Prebuild a column name String so tests don't repeatedly allocate it from scratch in setup code.
static COLUMN_NAME: Lazy<String> = Lazy::new(|| "test_col".to_string());

fn bench_monotonicity_asc(c: &mut Criterion) {
    let mut group = c.benchmark_group("monotonicity_asc");

    let rule = Monotonicity::new(true);

    for (size, arr) in PREBUILT_ARRAYS.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(format!("array_size_{}", size), arr, |b, arr_ref| {
            b.iter(|| {
                let col = COLUMN_NAME.clone();
                black_box(rule.validate(arr_ref.as_ref(), col).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_monotonicity_desc(c: &mut Criterion) {
    let mut group = c.benchmark_group("monotonicity_desc");

    let rule = Monotonicity::new(false);

    for (size, arr) in PREBUILT_ARRAYS.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(format!("array_size_{}", size), arr, |b, arr_ref| {
            b.iter(|| {
                let col = COLUMN_NAME.clone();
                black_box(rule.validate(arr_ref.as_ref(), col).unwrap());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_monotonicity_asc, bench_monotonicity_desc);
criterion_main!(benches);
