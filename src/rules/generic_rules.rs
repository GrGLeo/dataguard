use arrow::{
    array::Array,
    compute::{self},
    datatypes::DataType,
};
use arrow_array::StringArray;
use std::{collections::HashSet, sync::Arc};
use xxhash_rust::xxh3::xxh3_64;

use crate::{errors::RuleError, utils::hasher::Xxh3Builder};

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

    pub fn validate(&self, array: &StringArray) -> HashSet<u64, Xxh3Builder> {
        let mut local_hash = HashSet::with_hasher(Xxh3Builder);
        array.iter().for_each(|v_option| {
            if let Some(v) = v_option {
                let hash = xxh3_64(v.as_bytes());
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

    #[test]
    fn test_unicity_sequential_happy() {
        let rule = UnicityCheck::new();
        let dash = DashSet::with_hasher(Xxh3Builder::default());
        let array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let result = rule.validate(&array, &dash);
        assert!(result.is_ok());
        assert_eq!(dash.len(), 3);
    }

    #[test]
    fn test_unicity_parallel_happy() {
        let rule = UnicityCheck::new();
        let dash = Arc::new(DashSet::with_hasher(Xxh3Builder::default()));
        let arrays = vec![
            StringArray::from(vec![Some("a"), Some("b"), Some("c")]),
            StringArray::from(vec![Some("d"), Some("e"), Some("f")]),
        ];

        arrays.par_iter().for_each(|array| {
            let result = rule.clone().validate(array, &dash);
            assert!(result.is_ok());
        });

        assert_eq!(dash.len(), 6);
    }

    #[test]
    fn test_unicity_sequential_with_duplicates() {
        let rule = UnicityCheck::new();
        let dash = DashSet::with_hasher(Xxh3Builder::default());
        // "a" is duplicated, total unique values are "a", "b", "c"
        let array = StringArray::from(vec![Some("a"), Some("b"), Some("a"), Some("c")]);

        let result = rule.validate(&array, &dash);
        assert!(result.is_ok());
        assert_eq!(dash.len(), 3);
    }

    #[test]
    fn test_unicity_parallel_with_duplicates() {
        let rule = UnicityCheck::new();
        let dash = Arc::new(DashSet::with_hasher(Xxh3Builder::default()));
        let arrays = vec![
            // Duplicates within this array: "a"
            StringArray::from(vec![Some("a"), Some("b"), Some("a")]),
            // Duplicates across arrays: "b", and within this array: "c"
            StringArray::from(vec![Some("c"), Some("b"), Some("d"), Some("c")]),
        ];

        arrays.par_iter().for_each(|array| {
            let result = rule.clone().validate(array, &dash);
            assert!(result.is_ok());
        });

        // Total unique values should be "a", "b", "c", "d"
        assert_eq!(dash.len(), 4);
    }

    #[test]
    fn test_unicity_with_nulls() {
        let rule = UnicityCheck::new();
        let dash = DashSet::with_hasher(Xxh3Builder::default());
        // Nulls should be ignored
        let array = StringArray::from(vec![Some("a"), None, Some("b"), None, Some("a")]);

        let result = rule.validate(&array, &dash);
        assert!(result.is_ok());
        assert_eq!(dash.len(), 2);
    }
}
