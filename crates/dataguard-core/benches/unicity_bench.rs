use arrow::array::StringArray;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dataguard_core::rules::generic::UnicityCheck;
use once_cell::sync::Lazy;
use std::hint::black_box;
use std::sync::Arc;

/// Helper function to create a StringArray with a specific uniqueness ratio.
///
/// # Arguments
/// * `size` - Total number of elements in the array
/// * `unique_pct` - Percentage of unique values (0.0 to 1.0)
/// * `str_len` - Length of each string value
///
/// # Example
/// * `create_string_array_with_uniqueness(1000, 1.0, 32)` - 1000 unique 32-char strings
/// * `create_string_array_with_uniqueness(1000, 0.5, 32)` - 500 unique values, each repeated twice
fn create_string_array_with_uniqueness(
    size: usize,
    unique_pct: f64,
    str_len: usize,
) -> StringArray {
    let num_unique = ((size as f64) * unique_pct).max(1.0) as usize;
    let mut values = Vec::with_capacity(size);

    // Generate unique base values
    let unique_values: Vec<String> = (0..num_unique)
        .map(|i| {
            // Create a string with the specified length
            // Format as zero-padded number to ensure consistent length
            let modulus = 10usize.saturating_pow(str_len as u32);
            format!("{:0width$}", i % modulus, width = str_len)
        })
        .collect();

    // Fill array by cycling through unique values to create desired duplication
    for i in 0..size {
        let idx = i % num_unique;
        values.push(Some(unique_values[idx].clone()));
    }

    StringArray::from(values)
}

// Prebuild arrays for 100% unique scenario
// Worst case: HashSet grows to full array size
static ARRAYS_100PCT_UNIQUE: Lazy<Vec<(usize, Arc<StringArray>)>> = Lazy::new(|| {
    let sizes = [1_000usize, 10_000, 100_000, 300_000];
    let mut v = Vec::with_capacity(sizes.len());
    for &size in sizes.iter() {
        let arr = Arc::new(create_string_array_with_uniqueness(size, 1.0, 32));
        v.push((size, arr));
    }
    v
});

// Prebuild arrays for 50% unique scenario
// Realistic case: Moderate duplication
static ARRAYS_50PCT_UNIQUE: Lazy<Vec<(usize, Arc<StringArray>)>> = Lazy::new(|| {
    let sizes = [1_000usize, 10_000, 100_000, 300_000];
    let mut v = Vec::with_capacity(sizes.len());
    for &size in sizes.iter() {
        let arr = Arc::new(create_string_array_with_uniqueness(size, 0.5, 32));
        v.push((size, arr));
    }
    v
});

/// Benchmark unicity check with 100% unique values.
/// This represents the worst case for memory usage as the HashSet
/// must grow to contain all elements.
fn bench_unicity_100pct_unique(c: &mut Criterion) {
    let mut group = c.benchmark_group("unicity_100pct_unique");

    let rule = UnicityCheck::new(0.);

    for (size, arr) in ARRAYS_100PCT_UNIQUE.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), arr, |b, arr_ref| {
            b.iter(|| {
                let (null_count, _hash_set) = rule.validate_str(arr_ref.as_ref());
                // Only black_box the null_count as requested
                black_box(null_count);
            });
        });
    }

    group.finish();
}

/// Benchmark unicity check with 50% unique values.
/// This represents a realistic scenario with moderate duplication.
/// HashSet grows to half the array size.
fn bench_unicity_50pct_unique(c: &mut Criterion) {
    let mut group = c.benchmark_group("unicity_50pct_unique");

    let rule = UnicityCheck::new();

    for (size, arr) in ARRAYS_50PCT_UNIQUE.iter() {
        group.throughput(criterion::Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), arr, |b, arr_ref| {
            b.iter(|| {
                let (null_count, _hash_set) = rule.validate_str(arr_ref.as_ref());
                // Only black_box the null_count as requested
                black_box(null_count);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_unicity_100pct_unique,
    bench_unicity_50pct_unique
);
criterion_main!(benches);
