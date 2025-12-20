use std::marker::PhantomData;

use crate::{
    columns::{ColumnType, NumericType},
    ColumnRule,
};

#[derive(Debug, Clone)]
pub struct NumericColumnBuilder<T: NumericType> {
    name: String,
    rules: Vec<ColumnRule>,
    _phantom: PhantomData<T>,
}

impl<T: NumericType> ColumnBuilder for NumericColumnBuilder<T> {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn column_type(&self) -> ColumnType {
        T::column_type()
    }

    fn rules(&self) -> &[ColumnRule] {
        self.rules.as_slice()
    }
}

impl<T: NumericType> NumericColumnBuilder<T> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Add not null constraint
    pub fn is_not_null(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NullCheck);
        self
    }

    /// Add uniqueness constraint
    pub fn is_unique(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::Unicity);
        self
    }

    /// Set numeric range (both min and max)
    pub fn between(&mut self, min: T, max: T) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(min.to_f64()),
            max: Some(max.to_f64()),
        });
        self
    }

    /// Set minimum value
    pub fn min(&mut self, min: T) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(min.to_f64()),
            max: None,
        });
        self
    }

    /// Set maximum value
    pub fn max(&mut self, max: T) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: None,
            max: Some(max.to_f64()),
        });
        self
    }

    /// Check if values are positive (> 0)
    pub fn is_positive(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(T::positive_threshold()),
            max: None,
        });
        self
    }

    /// Check if values are negative (< 0)
    pub fn is_negative(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: None,
            max: Some(T::negative_threshold()),
        });
        self
    }

    /// Check if values are non-negative (>= 0)
    pub fn is_non_negative(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: Some(0.0),
            max: None,
        });
        self
    }

    /// Check if values are non-positive (<= 0)
    pub fn is_non_positive(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NumericRange {
            min: None,
            max: Some(0.0),
        });
        self
    }

    /// Check if values are monotonically increasing
    pub fn is_monotonically_increasing(&mut self) -> &mut Self {
        self.rules
            .push(ColumnRule::Monotonicity { ascending: true });
        self
    }

    /// Check if values are monotonically decreasing
    pub fn is_monotonically_decreasing(&mut self) -> &mut Self {
        self.rules
            .push(ColumnRule::Monotonicity { ascending: false });
        self
    }
}
