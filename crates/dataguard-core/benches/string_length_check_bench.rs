use arrow::array::StringArray;
use criterion::{criterion_group, criterion_main, Criterion};
use dataguard_core::rules::string::{StringLengthCheck, StringRule};
use once_cell::sync::Lazy;
use std::hint::black_box;
use std::sync::Arc;

// Helper function to create a StringArray with a given size and average string length
fn create_string_array(size: usize, avg_len: usize) -> StringArray {
    let strings: Vec<Option<String>> = (0..size)
        .map(|i| {
            // Produce a fixed-width numeric string to approximate avg_len characters.
            // This is simple and deterministic for benchmarking.
            let modulus = 10usize.saturating_pow(avg_len as u32);
            let s = format!("{:0width$}", i % modulus, width = avg_len);
            Some(s)
        })
        .collect();
    StringArray::from_iter(strings)
}

// Prebuild arrays once and reuse across all benchmark functions.
static PREBUILT_ARRAYS: Lazy<Vec<(usize, Arc<StringArray>)>> = Lazy::new(|| {
    let sizes = [1_000usize, 10_000, 100_000, 300_000];
    let mut v = Vec::with_capacity(sizes.len());
    for &size in sizes.iter() {
        // Choose a reasonable average length for generation (8 here); you can tweak per-case below if needed
        let arr = Arc::new(create_string_array(size, 8));
        v.push((size, arr));
    }
    v
});

// Prebuild a column name String so tests don't repeatedly allocate it from scratch in setup code.
static COLUMN_NAME: Lazy<String> = Lazy::new(|| "test_col".to_string());

fn bench_string_length_between(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_length_check_between");

    let min_len = Some(5usize);
    let max_len = Some(10usize);
    let rule = StringLengthCheck::new(min_len, max_len);

    for (size, arr) in PREBUILT_ARRAYS.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        // Pass the Arc<StringArray> directly as input. bench_with_input receives &T, so the closure gets &Arc<StringArray>
        group.bench_with_input(format!("array_size_{}", size), arr, |b, arr_ref| {
            b.iter(|| {
                let col = COLUMN_NAME.clone();
                black_box(rule.validate(arr_ref.as_ref(), col).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_string_length_min(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_length_check_min");

    let min_len = Some(5usize);
    let max_len = None;
    let rule = StringLengthCheck::new(min_len, max_len);

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

fn bench_string_length_max(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_length_check_max");

    let min_len = None;
    let max_len = Some(5usize);
    let rule = StringLengthCheck::new(min_len, max_len);

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

criterion_group!(
    benches,
    bench_string_length_min,
    bench_string_length_between,
    bench_string_length_max
);
criterion_main!(benches);
