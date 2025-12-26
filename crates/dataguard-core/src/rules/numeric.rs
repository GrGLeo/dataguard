use arrow::array::Array;
use arrow_array::{ArrowNumericType, PrimitiveArray};
use arrow_ord::cmp::{gt, lt};
use num_traits::Num;
use std::{fmt::Debug, marker::PhantomData};

use crate::{columns::NumericType, engine::Stats, errors::RuleError};

pub trait NumericRule<T: ArrowNumericType>: Send + Sync {
    /// Returns the name of the rule.
    fn name(&self) -> String;
    /// Returns the rule threshold
    fn get_threshold(&self) -> f64;
    /// Validates an Arrow `Array`.
    fn validate(&self, array: &PrimitiveArray<T>, column: String) -> Result<usize, RuleError>;
    /// Validates an Arrow `Array`, by statistics
    fn validate_with_stats(&self, array: &PrimitiveArray<T>, stats: &Stats) -> usize;
}

pub struct Range<N: Num + PartialOrd + Copy + Debug> {
    name: String,
    threshold: f64,
    min: Option<N>,
    max: Option<N>,
}

impl<N> Range<N>
where
    N: Num + PartialOrd + Copy + Debug,
{
    pub fn new(name: String, threshold: f64, min: Option<N>, max: Option<N>) -> Self {
        Self {
            name,
            threshold,
            min,
            max,
        }
    }
}

impl<T, N> NumericRule<T> for Range<N>
where
    T: ArrowNumericType<Native = N>,
    N: Num + PartialOrd + Copy + Debug + Send + Sync,
{
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_threshold(&self) -> f64 {
        self.threshold
    }

    fn validate(&self, array: &PrimitiveArray<T>, _column: String) -> Result<usize, RuleError> {
        let mut counter: usize = 0;
        for value in array.iter() {
            match value {
                Some(i) => {
                    if let Some(min) = self.min {
                        if i < min {
                            counter += 1
                        }
                    }
                    if let Some(max) = self.max {
                        if i > max {
                            counter += 1
                        }
                    }
                }
                None => counter += 0, // Null doesnt count as error
            }
        }
        Ok(counter)
    }

    fn validate_with_stats(&self, _array: &PrimitiveArray<T>, _stats: &Stats) -> usize {
        // We should never call this method on this rule
        // It it's happen we panic and fix this case
        unreachable!()
    }
}

pub struct Monotonicity<N> {
    name: String,
    threshold: f64,
    asc: bool,
    _phantom: PhantomData<N>, // To tie N to the struct
}

impl<N: PartialOrd> Monotonicity<N> {
    pub fn new(name: String, threshold: f64, asc: bool) -> Self {
        Self {
            name,
            threshold,
            asc,
            _phantom: PhantomData,
        }
    }
}

impl<N: PartialOrd> Default for Monotonicity<N> {
    fn default() -> Self {
        Self {
            name: "IsIngreasing".to_string(),
            threshold: 0.,
            asc: true,
            _phantom: PhantomData,
        }
    }
}

impl<T, N> NumericRule<T> for Monotonicity<N>
where
    T: ArrowNumericType<Native = N>,
    N: PartialOrd + Copy + Debug + Send + Sync,
{
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_threshold(&self) -> f64 {
        self.threshold
    }

    fn validate(&self, array: &PrimitiveArray<T>, _column: String) -> Result<usize, RuleError> {
        if array.len() <= 1 {
            return Ok(0);
        };
        let predecessor = array.slice(0, array.len() - 1);
        let successor = array.slice(1, array.len() - 1);

        let predecessor_array = predecessor
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();
        let successor_array = successor
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap();

        let comparaison = match self.asc {
            true => lt(successor_array, predecessor_array),
            false => gt(successor_array, predecessor_array),
        };
        let violation = comparaison.map_err(RuleError::ArrowError)?.true_count();
        Ok(violation)
    }

    fn validate_with_stats(&self, _array: &PrimitiveArray<T>, _stats: &Stats) -> usize {
        // We should never call this method on this rule
        // It it's happen we panic and fix this case
        unreachable!()
    }
}

pub struct StdDevCheck {
    name: String,
    threshold: f64,
    max_std_dev: f64,
}

impl StdDevCheck {
    pub fn new(name: String, threshold: f64, max_std_dev: f64) -> Self {
        Self {
            name,
            threshold,
            max_std_dev,
        }
    }
}

impl<T, N> NumericRule<T> for StdDevCheck
where
    T: ArrowNumericType<Native = N>,
    N: NumericType,
{
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_threshold(&self) -> f64 {
        self.threshold
    }

    fn validate(&self, _array: &PrimitiveArray<T>, _column: String) -> Result<usize, RuleError> {
        unreachable!()
    }

    fn validate_with_stats(&self, array: &PrimitiveArray<T>, stats: &Stats) -> usize {
        let mut counter = 0;
        let mean = stats.mean();
        let std_dev = stats.std_dev();

        if std_dev == 0. {
            return 0;
        }
        for v in array.iter().flatten() {
            let v_f64 = v.to_f64();
            let z_score = (v_f64 - mean).abs() / std_dev;
            counter += (z_score >= self.max_std_dev) as usize;
        }
        counter
    }
}

pub struct MeanVarianceCheck {
    name: String,
    threshold: f64,
    max_variance_percent: f64,
}

impl MeanVarianceCheck {
    pub fn new(name: String, threshold: f64, max_variance_percent: f64) -> Self {
        Self {
            name,
            threshold,
            max_variance_percent,
        }
    }
}

impl<T, N> NumericRule<T> for MeanVarianceCheck
where
    T: ArrowNumericType<Native = N>,
    N: NumericType,
{
    fn name(&self) -> String {
        self.name.clone()
    }

    fn get_threshold(&self) -> f64 {
        self.threshold
    }

    fn validate(&self, _array: &PrimitiveArray<T>, _column: String) -> Result<usize, RuleError> {
        unreachable!()
    }

    fn validate_with_stats(&self, array: &PrimitiveArray<T>, stats: &Stats) -> usize {
        let mut counter = 0;
        let mean = stats.mean();
        let bound = mean * (self.max_variance_percent / 100.);

        for v in array.iter().flatten() {
            let v_f64 = v.to_f64();
            let diff = (mean - v_f64).abs();
            counter += (diff > bound) as usize;
        }
        counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow_array::Float64Array;

    #[test]
    fn test_min_range_integer_with_null() {
        let rule = Range::new("range_test".to_string(), 0.0, Some(5i64), None);
        let array = Int64Array::from(vec![Some(1), Some(6), Some(3), Some(2), None]);
        // We expect 4 errors here index 0, 2, 3
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_min_range_integer() {
        let rule = Range::new("range_test".to_string(), 0.0, Some(5i64), None);
        let array = Int64Array::from(vec![Some(7), Some(6), Some(5), Some(2), Some(4)]);
        // We expect 2 errors here index 3, 4
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_max_range_integer_with_null() {
        let rule = Range::new("range_test".to_string(), 0.0, None, Some(5i64));
        let array = Int64Array::from(vec![Some(1), Some(6), Some(3), Some(2), None]);
        // We expect 1 errors here index 1
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_max_range_integer() {
        let rule = Range::new("range_test".to_string(), 0.0, None, Some(5i64));
        let array = Int64Array::from(vec![Some(7), Some(6), Some(5), Some(2), Some(4)]);
        // We expect 2 errors here index 0, 1
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_range_between_integer_with_null() {
        let rule = Range::new("range_test".to_string(), 0.0, Some(2i64), Some(4i64));
        let array = Int64Array::from(vec![Some(1), Some(4), Some(6), Some(3), Some(2), None]);
        // We expect 2 errors here: 0, 2
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_range_between_integer() {
        let rule = Range::new("range_test".to_string(), 0.0, Some(2i64), Some(4i64));
        let array = Int64Array::from(vec![Some(7), Some(6), Some(5), Some(2), Some(4)]);
        // We expect 2 errors here index 0, 1, 2
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 3);
    }

    #[test]
    fn test_monotonicity_asc_valid() {
        let rule = Monotonicity::<i64>::default();
        let array = Int64Array::from(vec![1, 5, 5, 10]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_monotonicity_asc_violation() {
        let rule = Monotonicity::<i64>::default();
        let array = Int64Array::from(vec![1, 5, 4, 3, 10]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_monotonicity_desc_valid() {
        let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), 0.0, false);
        //
        //
        let array = Int64Array::from(vec![10, 5, 5, 1]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 0);
    }

    #[test]
    fn test_monotonicity_desc_violation() {
        let rule = Monotonicity::<i64>::new("monotonicity_test".to_string(), 0.0, false);
        let array = Int64Array::from(vec![10, 3, 4, 5, 1]);
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_is_positive() {
        let rule = Range::new("range_test".to_string(), 0.0, Some(1i64), None);
        let array = Int64Array::from(vec![Some(1), Some(0), Some(5), Some(-2), None]);
        // 0, -2 should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_is_negative() {
        let rule = Range::new("range_test".to_string(), 0.0, None, Some(-1i64));
        let array = Int64Array::from(vec![Some(-1), Some(0), Some(-5), Some(2), None]);
        // 0, 2 should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 2);
    }

    #[test]
    fn test_is_non_positive() {
        let rule = Range::new("range_test".to_string(), 0.0, None, Some(0i64));
        let array = Int64Array::from(vec![Some(-1), Some(0), Some(5), Some(-2), None]);
        // 5 should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_is_non_negative() {
        let rule = Range::new("range_test".to_string(), 0.0, Some(0i64), None);
        let array = Int64Array::from(vec![Some(1), Some(0), Some(5), Some(-2), None]);
        // -2 should be violations
        assert_eq!(rule.validate(&array, "test_col".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_int_std_dev_check() {
        let rule = StdDevCheck::new("std_dev_check".to_string(), 0.0, 2.0);
        let array = Int64Array::from(vec![Some(90), Some(25), Some(15), Some(35), None, Some(-2)]);
        let stats = Stats::Integer {
            count: 10,
            mean: 45.,
            m2: 1000.,
            min: 2,
            max: 2,
        };
        // std_dev +/- 10 | fail, pass, fail, pass, _, fail
        let violations = rule.validate_with_stats(&array, &stats);

        assert_eq!(violations, 3)
    }

    #[test]
    fn test_float_std_dev_check() {
        let rule = StdDevCheck::new("std_dev_check".to_string(), 0.0, 2.0);
        let array = Float64Array::from(vec![
            Some(90.),
            Some(25.),
            Some(15.),
            Some(35.),
            None,
            Some(-2.),
        ]);
        let stats = Stats::Float {
            mean: 45.,
            count: 10,
            m2: 1000.,
            min: 2.,
            max: 2.,
        };
        // std_dev +/- 10 | fail, pass, fail, pass, _, fail
        let violations = rule.validate_with_stats(&array, &stats);

        assert_eq!(violations, 3)
    }

    #[test]
    fn test_int_mean_var() {
        let rule = MeanVarianceCheck::new("mean_variance_check".to_string(), 0.0, 20.0);
        let array = Int64Array::from(vec![
            Some(90),
            Some(36),
            Some(15),
            Some(35),
            None,
            Some(-2),
            Some(54),
        ]);
        let stats = Stats::Integer {
            count: 10,
            mean: 45.,
            m2: 1000.,
            min: 2,
            max: 2,
        };
        // bound = 9. | fail, pass, fail, fail, _, fail, pass
        let violations = rule.validate_with_stats(&array, &stats);

        assert_eq!(violations, 4)
    }

    #[test]
    fn test_float_mean_var() {
        let rule = MeanVarianceCheck::new("mean_variance_check".to_string(), 0.0, 20.0);
        let array = Float64Array::from(vec![
            Some(90.),
            Some(36.),
            Some(15.),
            Some(35.),
            None,
            Some(-2.),
            Some(54.),
        ]);
        let stats = Stats::Float {
            count: 10,
            mean: 45.,
            m2: 1000.,
            min: 2.,
            max: 2.,
        };
        // bound = 9. | fail, pass, fail, fail, _, fail, pass
        let violations = rule.validate_with_stats(&array, &stats);

        assert_eq!(violations, 4)
    }
}
