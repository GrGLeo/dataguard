use arrow::{
    array::Array,
    compute::{self},
    datatypes::{DataType, ToByteSlice},
};
use arrow_array::{ArrowPrimitiveType, PrimitiveArray, StringArray};
use std::{collections::HashSet, sync::Arc};
use xxhash_rust::xxh3::xxh3_64;

use crate::{errors::RuleError, utils::hasher::Xxh3Builder};

pub struct NullCheck {}

impl NullCheck {
    pub fn new() -> Self {
        Self {}
    }

    pub fn name(&self) -> &'static str {
        "NullCheck"
    }

    pub fn validate(&self, array: &dyn Array) -> usize {
        array.null_count()
    }
}

impl Default for NullCheck {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TypeCheck {
    column: String,
    expected: DataType,
}

impl TypeCheck {
    pub fn new(column: String, expected: DataType) -> Self {
        Self { column, expected }
    }

    pub fn name(&self) -> &'static str {
        "TypeCheck"
    }

    pub fn validate(&self, array: &dyn Array) -> Result<(usize, Arc<dyn Array>), RuleError> {
        match compute::cast(array, &self.expected) {
            Ok(casted_array) => {
                let errors = casted_array.null_count() - array.null_count();
                Ok((errors, casted_array))
            }
            Err(e) => Err(RuleError::TypeCastError(self.column.clone(), e.to_string())),
        }
    }
}

#[derive(Clone)]
pub struct UnicityCheck {}

impl Default for UnicityCheck {
    fn default() -> Self {
        Self::new()
    }
}

impl UnicityCheck {
    pub fn new() -> Self {
        Self {}
    }

    pub fn name(&self) -> &'static str {
        "UnicityCheck"
    }

    pub fn validate_str(&self, array: &StringArray) -> HashSet<u64, Xxh3Builder> {
        let mut local_hash = HashSet::with_hasher(Xxh3Builder);
        array.iter().for_each(|v_option| {
            if let Some(v) = v_option {
                let hash = xxh3_64(v.as_bytes());
                let _ = local_hash.insert(hash);
            }
        });
        local_hash
    }

    pub fn validate_numeric<T: ArrowPrimitiveType>(
        &self,
        array: &PrimitiveArray<T>,
    ) -> HashSet<u64, Xxh3Builder> {
        let mut local_hash = HashSet::with_hasher(Xxh3Builder);
        array.iter().for_each(|v_option| {
            if let Some(v) = v_option {
                let hash = xxh3_64(v.to_byte_slice());
                let _ = local_hash.insert(hash);
            }
        });
        local_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;

    // DashSet and Arc are no longer directly used in tests for UnicityCheck
    // as the validate method now returns a HashSet.

    #[test]
    fn test_unicity_sequential_happy() {
        let rule = UnicityCheck::new();
        let array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let local_set = rule.validate_str(&array);
        assert_eq!(local_set.len(), 3);
        assert!(local_set.contains(&xxh3_64("a".as_bytes())));
        assert!(local_set.contains(&xxh3_64("b".as_bytes())));
        assert!(local_set.contains(&xxh3_64("c".as_bytes())));
    }

    #[test]
    fn test_unicity_parallel_happy() {
        let rule = UnicityCheck::new();
        let arrays = vec![
            StringArray::from(vec![Some("a"), Some("b"), Some("c")]),
            StringArray::from(vec![Some("d"), Some("e"), Some("f")]),
        ];

        let final_set = arrays
            .par_iter()
            .map(|array| rule.validate_str(array)) // Each map call returns a HashSet for its array
            .reduce(
                || HashSet::with_hasher(Xxh3Builder), // Identity for reduce
                |mut acc_set, batch_set| {
                    // Accumulator for reduce
                    acc_set.extend(batch_set);
                    acc_set
                },
            );

        assert_eq!(final_set.len(), 6);
        assert!(final_set.contains(&xxh3_64("a".as_bytes())));
        assert!(final_set.contains(&xxh3_64("b".as_bytes())));
        assert!(final_set.contains(&xxh3_64("c".as_bytes())));
        assert!(final_set.contains(&xxh3_64("d".as_bytes())));
        assert!(final_set.contains(&xxh3_64("e".as_bytes())));
        assert!(final_set.contains(&xxh3_64("f".as_bytes())));
    }

    #[test]
    fn test_unicity_sequential_with_duplicates() {
        let rule = UnicityCheck::new();
        let array = StringArray::from(vec![Some("a"), Some("b"), Some("a"), Some("c")]);

        let local_set = rule.validate_str(&array);
        assert_eq!(local_set.len(), 3); // "a", "b", "c" are unique
        assert!(local_set.contains(&xxh3_64("a".as_bytes())));
        assert!(local_set.contains(&xxh3_64("b".as_bytes())));
        assert!(local_set.contains(&xxh3_64("c".as_bytes())));
    }

    #[test]
    fn test_unicity_parallel_with_duplicates() {
        let rule = UnicityCheck::new();
        let arrays = vec![
            StringArray::from(vec![Some("a"), Some("b"), Some("a")]),
            StringArray::from(vec![Some("c"), Some("b"), Some("d"), Some("c")]),
        ];

        let final_set = arrays
            .par_iter()
            .map(|array| rule.validate_str(array))
            .reduce(
                || HashSet::with_hasher(Xxh3Builder),
                |mut acc_set, batch_set| {
                    acc_set.extend(batch_set);
                    acc_set
                },
            );

        assert_eq!(final_set.len(), 4); // "a", "b", "c", "d" are unique
        assert!(final_set.contains(&xxh3_64("a".as_bytes())));
        assert!(final_set.contains(&xxh3_64("b".as_bytes())));
        assert!(final_set.contains(&xxh3_64("c".as_bytes())));
        assert!(final_set.contains(&xxh3_64("d".as_bytes())));
    }

    #[test]
    fn test_unicity_with_nulls() {
        let rule = UnicityCheck::new();
        let array = StringArray::from(vec![Some("a"), None, Some("b"), None, Some("a")]);

        let local_set = rule.validate_str(&array);
        assert_eq!(local_set.len(), 2); // Nulls are ignored, "a", "b" are unique
        assert!(local_set.contains(&xxh3_64("a".as_bytes())));
        assert!(local_set.contains(&xxh3_64("b".as_bytes())));
    }
}
