#![allow(dead_code)]
use std::collections::HashMap;

use arrow_array::{
    types::{Float64Type, Int64Type},
    PrimitiveArray,
};

use crate::columns::NumericType;

/// Statistics for a column, stored with the appropriate native type
#[derive(Debug, Clone)]
pub enum Stats {
    Integer {
        count: usize,
        mean: f64,
        m2: f64,
        min: i64,
        max: i64,
    },
    Float {
        count: usize,
        mean: f64,
        m2: f64,
        min: f64,
        max: f64,
    },
}

impl Stats {
    pub fn count(&self) -> usize {
        match self {
            Stats::Integer { count, .. } => *count,
            Stats::Float { count, .. } => *count,
        }
    }

    pub fn mean(&self) -> f64 {
        match self {
            Stats::Integer { mean, .. } => *mean,
            Stats::Float { mean, .. } => *mean,
        }
    }

    /// Standard deviation using sample variance (N-1)
    pub fn std_dev(&self) -> f64 {
        self.sample_variance().sqrt()
    }

    /// Sample variance (divides by N-1)
    pub fn sample_variance(&self) -> f64 {
        let (count, m2) = match self {
            Stats::Integer { count, m2, .. } => (*count, *m2),
            Stats::Float { count, m2, .. } => (*count, *m2),
        };

        if count < 2 {
            0.0
        } else {
            m2 / (count - 1) as f64
        }
    }

    /// Population variance (divides by N)
    pub fn population_variance(&self) -> f64 {
        let (count, m2) = match self {
            Stats::Integer { count, m2, .. } => (*count, *m2),
            Stats::Float { count, m2, .. } => (*count, *m2),
        };

        if count < 2 {
            0.0
        } else {
            m2 / count as f64
        }
    }
}

/// Accumulator for computing statistics across multiple columns
pub struct StatsAccumulator {
    pub columns: HashMap<String, Stats>,
}

impl StatsAccumulator {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    pub fn get(&self, column: &str) -> Option<&Stats> {
        self.columns.get(column)
    }

    /// Update statistics for an integer column
    pub fn update_integer(&mut self, column_name: &str, array: &PrimitiveArray<Int64Type>) {
        let stats = self
            .columns
            .entry(column_name.to_string())
            .or_insert(Stats::Integer {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                min: i64::MAX,
                max: i64::MIN,
            });

        match stats {
            Stats::Integer {
                count,
                mean,
                m2,
                min,
                max,
            } => {
                for &value in array.values() {
                    // Welford's algorithm for mean and m2
                    Self::update_welford(count, mean, m2, value);

                    // Update min/max with native type
                    Self::update_min_max(count, min, max, value);
                }
            }
            Stats::Float { .. } => {
                panic!(
                    "Type mismatch: expected Integer stats for column {}",
                    column_name
                );
            }
        }
    }

    /// Update statistics for a float column
    pub fn update_float(&mut self, column_name: &str, array: &PrimitiveArray<Float64Type>) {
        let stats = self
            .columns
            .entry(column_name.to_string())
            .or_insert(Stats::Float {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                min: f64::MAX,
                max: f64::MIN,
            });

        match stats {
            Stats::Float {
                count,
                mean,
                m2,
                min,
                max,
            } => {
                for &value in array.values() {
                    // Welford's algorithm for mean and m2
                    Self::update_welford(count, mean, m2, value);

                    // Update min/max with native type
                    Self::update_min_max(count, min, max, value);
                }
            }
            Stats::Integer { .. } => {
                panic!(
                    "Type mismatch: expected Float stats for column {}",
                    column_name
                );
            }
        }
    }

    /// Welford's algorithm for running mean and variance
    #[inline]
    fn update_welford<N>(count: &mut usize, mean: &mut f64, m2: &mut f64, value: N)
    where
        N: NumericType,
    {
        *count += 1;
        let value_f64 = value.to_f64();
        let delta = value_f64 - *mean;
        *mean += delta / *count as f64;
        let delta2 = value_f64 - *mean;
        *m2 += delta * delta2;
    }

    /// Update min and max values
    #[inline]
    fn update_min_max<N>(count: &usize, min: &mut N, max: &mut N, value: N)
    where
        N: PartialOrd + Copy,
    {
        if *count == 1 {
            *min = value;
            *max = value;
        } else {
            if value < *min {
                *min = value;
            }
            if value > *max {
                *max = value;
            }
        }
    }
}

impl Default for StatsAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Merges two [`Stats`] objects into a single combined statistical summary.
///
/// This function uses Chan's parallel algorithm to combine the count, mean,
/// and sum of squares of two independent sets of data in a numerically
/// stable manner.
///
/// # Mathematical Logic
/// The combined mean and combined are calculated as:
/// - n_c = n_a + n_b
/// - mu_c = (n_a * mu_a + n_b * mu_b)/n_c
/// - M2c = M2a + M2b + (mu_b - mu_a)^2 * (n_a * n_b)/n_c
///
/// # Numerical Stability
/// To maintain precision when merging two large datasets (n_A ≈ n_B),
/// this implementation uses the weighted average of means rather than
/// delta-based updates. This minimizes floating-point drift common in
/// large-scale parallel reductions.
///
/// # Panics
/// Panics if the variants of `a` and `b` do not match (e.g., trying to merge
/// `Stats::Integer` with `Stats::Float`).
pub fn merge_stats(a: Stats, b: Stats) -> Stats {
    match (a, b) {
        (
            Stats::Integer {
                count: ca,
                mean: ma,
                m2: m2a,
                min: mina,
                max: maxa,
            },
            Stats::Integer {
                count: cb,
                mean: mb,
                m2: m2b,
                min: minb,
                max: maxb,
            },
        ) => {
            // Chan's algorithm for merging
            let count_c = ca + cb;
            if count_c == 0 {
                return Stats::Integer {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    min: i64::MAX,
                    max: i64::MIN,
                };
            }

            let mean_c = (ca as f64 * ma + cb as f64 * mb) / count_c as f64;
            let delta = mb - ma;
            let m2_c = m2a + m2b + delta * delta * (ca * cb) as f64 / count_c as f64;

            Stats::Integer {
                count: count_c,
                mean: mean_c,
                m2: m2_c,
                min: mina.min(minb),
                max: maxa.max(maxb),
            }
        }
        (
            Stats::Float {
                count: ca,
                mean: ma,
                m2: m2a,
                min: mina,
                max: maxa,
            },
            Stats::Float {
                count: cb,
                mean: mb,
                m2: m2b,
                min: minb,
                max: maxb,
            },
        ) => {
            let count_c = ca + cb;
            if count_c == 0 {
                return Stats::Float {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    min: f64::MAX,
                    max: f64::MIN,
                };
            }

            let mean_c = (ca as f64 * ma + cb as f64 * mb) / count_c as f64;
            let delta = mb - ma;
            let m2_c = m2a + m2b + delta * delta * (ca * cb) as f64 / count_c as f64;

            Stats::Float {
                count: count_c,
                mean: mean_c,
                m2: m2_c,
                min: mina.min(minb),
                max: maxa.max(maxb),
            }
        }
        _ => panic!("Cannot merge Integer and Float stats"),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array};

    use super::*;

    #[test]
    fn test_integer_stats_basic() {
        let mut acc = StatsAccumulator::new();
        let arr = Int64Array::from(vec![1, 2, 3, 4, 5]);

        acc.update_integer("age", &arr);

        let stats = acc.columns.get("age").unwrap();
        assert_eq!(stats.count(), 5);
        assert_eq!(stats.mean(), 3.0);

        match stats {
            Stats::Integer { min, max, .. } => {
                assert_eq!(*min, 1);
                assert_eq!(*max, 5);
            }
            _ => panic!("Expected Integer stats"),
        }
    }

    #[test]
    fn test_float_stats_basic() {
        let mut acc = StatsAccumulator::new();
        let arr = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);

        acc.update_float("price", &arr);

        let stats = acc.columns.get("price").unwrap();
        assert_eq!(stats.count(), 5);
        assert_eq!(stats.mean(), 3.0);

        match stats {
            Stats::Float { min, max, .. } => {
                assert_eq!(*min, 1.0);
                assert_eq!(*max, 5.0);
            }
            _ => panic!("Expected Float stats"),
        }
    }

    #[test]
    #[allow(deprecated)]
    fn test_welford_variance() {
        let mut acc = StatsAccumulator::new();
        let arr = Int64Array::from(vec![1, 2, 3, 4, 5]);

        acc.update_integer("test", &arr);

        let stats = acc.columns.get("test").unwrap();
        assert_eq!(stats.sample_variance(), 2.5);
        assert!((stats.std_dev() - 1.58113883).abs() < 1e-7);
        assert_eq!(stats.population_variance(), 2.0);
    }

    #[test]
    fn test_multiple_columns() {
        let mut acc = StatsAccumulator::new();

        let ages = Int64Array::from(vec![25, 30, 35, 40]);
        let prices = Float64Array::from(vec![10.5, 20.5, 30.5]);

        acc.update_integer("age", &ages);
        acc.update_float("price", &prices);

        assert_eq!(acc.columns.len(), 2);
        assert_eq!(acc.columns.get("age").unwrap().mean(), 32.5);
        assert_eq!(acc.columns.get("price").unwrap().mean(), 20.5);
    }

    #[test]
    fn test_incremental_updates() {
        let mut acc = StatsAccumulator::new();

        let batch1 = Int64Array::from(vec![1, 2, 3]);
        let batch2 = Int64Array::from(vec![4, 5]);

        acc.update_integer("values", &batch1);
        acc.update_integer("values", &batch2);

        let stats = acc.columns.get("values").unwrap();
        assert_eq!(stats.count(), 5);
        assert_eq!(stats.mean(), 3.0);
    }

    #[test]
    fn test_edge_case_single_value() {
        let mut acc = StatsAccumulator::new();
        let arr = Int64Array::from(vec![42]);

        acc.update_integer("single", &arr);

        let stats = acc.columns.get("single").unwrap();
        assert_eq!(stats.count(), 1);
        assert_eq!(stats.mean(), 42.0);
        assert_eq!(stats.sample_variance(), 0.0);
        assert_eq!(stats.std_dev(), 0.0);
    }

    #[test]
    fn test_edge_case_constant_values() {
        let mut acc = StatsAccumulator::new();
        let arr = Int64Array::from(vec![5, 5, 5, 5, 5]);

        acc.update_integer("constant", &arr);

        let stats = acc.columns.get("constant").unwrap();
        assert_eq!(stats.mean(), 5.0);
        assert_eq!(stats.sample_variance(), 0.0);
        assert_eq!(stats.std_dev(), 0.0);
    }

    #[test]
    fn test_two_batch_accumulation() {
        let mut acc = StatsAccumulator::new();
        // [1,2,3,4,5,6,7]: mean=4, sample_var=4.666..., pop_var=4.0
        let arr1 = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let arr2 = Int64Array::from(vec![6, 7]);

        acc.update_integer("constant", &arr1);
        acc.update_integer("constant", &arr2);

        let stats = acc.columns.get("constant").unwrap();
        assert_eq!(stats.mean(), 4.0);
        assert!((stats.sample_variance() - 4.666666).abs() < 1e-5);
        assert!((stats.std_dev() - 2.16024).abs() < 1e-5);
        assert_eq!(stats.population_variance(), 4.0);
    }

    #[test]
    #[should_panic(expected = "Type mismatch")]
    fn test_type_mismatch_panic() {
        let mut acc = StatsAccumulator::new();

        let arr1 = Int64Array::from(vec![1, 2, 3]);
        acc.update_integer("col", &arr1);

        // col is set as a integer
        let arr2 = Float64Array::from(vec![1.0, 2.0, 3.0]);
        acc.update_float("col", &arr2);
    }
}

/// Merges two [`Stats`] objects into a single combined statistical summary.
///
/// This function uses Chan's parallel algorithm to combine the count, mean,
/// and sum of squares of two independent sets of data in a numerically
/// stable manner.
///
/// # Mathematical Logic
/// The combined mean and combined are calculated as:
/// - n_c = n_a + n_b
/// - mu_c = (n_a * mu_a + n_b * mu_b)/n_c
/// - M2c = M2a + M2b + (mu_b - mu_a)^2 * (n_a * n_b)/n_c
///
/// # Panics
/// Panics if the variants of `a` and `b` do not match (e.g., trying to merge
/// `Stats::Integer` with `Stats::Float`).
pub fn merge_stats(a: Stats, b: Stats) -> Stats {
    match (a, b) {
        (
            Stats::Integer {
                count: ca,
                mean: ma,
                m2: m2a,
                min: mina,
                max: maxa,
            },
            Stats::Integer {
                count: cb,
                mean: mb,
                m2: m2b,
                min: minb,
                max: maxb,
            },
        ) => {
            // Chan's algorithm for merging
            let count_c = ca + cb;
            if count_c == 0 {
                return Stats::Integer {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    min: i64::MAX,
                    max: i64::MIN,
                };
            }

            let mean_c = (ca as f64 * ma + cb as f64 * mb) / count_c as f64;
            let delta = mb - ma;
            let m2_c = m2a + m2b + delta * delta * (ca * cb) as f64 / count_c as f64;

            Stats::Integer {
                count: count_c,
                mean: mean_c,
                m2: m2_c,
                min: mina.min(minb),
                max: maxa.max(maxb),
            }
        }
        (
            Stats::Float {
                count: ca,
                mean: ma,
                m2: m2a,
                min: mina,
                max: maxa,
            },
            Stats::Float {
                count: cb,
                mean: mb,
                m2: m2b,
                min: minb,
                max: maxb,
            },
        ) => {
            let count_c = ca + cb;
            if count_c == 0 {
                return Stats::Float {
                    count: 0,
                    mean: 0.0,
                    m2: 0.0,
                    min: f64::MAX,
                    max: f64::MIN,
                };
            }

            let mean_c = (ca as f64 * ma + cb as f64 * mb) / count_c as f64;
            let delta = mb - ma;
            let m2_c = m2a + m2b + delta * delta * (ca * cb) as f64 / count_c as f64;

            Stats::Float {
                count: count_c,
                mean: mean_c,
                m2: m2_c,
                min: mina.min(minb),
                max: maxa.max(maxb),
            }
        }
        _ => panic!("Cannot merge Integer and Float stats"),
    }
}
