use std::sync::Arc;

use arrow_array::{Array, Date32Array};

use crate::{utils::operator::CompOperator, RuleError};

pub trait RelationRule: Send + Sync {
    fn name(&self) -> String;
    fn get_threshold(&self) -> f64;
    fn validate(
        &self,
        lhs: &Arc<dyn Array>,
        rhs: &Arc<dyn Array>,
        columns: [&str; 2],
    ) -> Result<usize, RuleError>;
}

pub struct DateCompareCheck {
    op: CompOperator,
}

impl DateCompareCheck {
    pub fn new(op: CompOperator) -> Self {
        Self { op }
    }
}

impl RelationRule for DateCompareCheck {
    fn name(&self) -> String {
        format!("DateCompare{}", self.op)
    }

    fn get_threshold(&self) -> f64 {
        0.
    }

    fn validate(
        &self,
        lhs: &Arc<dyn Array>,
        rhs: &Arc<dyn Array>,
        column: [&str; 2],
    ) -> Result<usize, RuleError> {
        // There might be a compute kernel but null on either side return a null when we want to
        // record a violation
        let opt_lhs = lhs.as_any().downcast_ref::<Date32Array>();
        let opt_rhs = rhs.as_any().downcast_ref::<Date32Array>();
        match (opt_lhs, opt_rhs) {
            (Some(lhs), Some(rhs)) => {
                let compare = self.op.get_comparator::<i32>();
                let mut violations: usize = 0;
                for (i, j) in lhs.iter().zip(rhs) {
                    match (i, j) {
                        (Some(a), Some(b)) => {
                            if !compare(a, b) {
                                violations += 1
                            }
                        }
                        (Some(_), None) => violations += 1,
                        (None, Some(_)) => violations += 1,
                        (None, None) => {}
                    }
                }
                Ok(violations)
            }
            (None, Some(_)) => Err(RuleError::TypeCastError(
                column[0].to_string(),
                "Date32Array".to_string(),
            )),
            (Some(_), None) => Err(RuleError::TypeCastError(
                column[0].to_string(),
                "Date32Array".to_string(),
            )),
            (None, None) => Err(RuleError::TypeCastError(
                format!("{} and {}", column[0], column[1]),
                "Date32Array".to_string(),
            )),
        }
    }
}
