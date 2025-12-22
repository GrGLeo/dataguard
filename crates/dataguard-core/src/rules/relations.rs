use std::sync::Arc;

use arrow_array::{Array, Date32Array};

use crate::{utils::operator::CompOperator, RuleError};

pub trait RelationRule: Send + Sync {
    fn name(&self) -> &'static str;
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
    fn name(&self) -> &'static str {
        "DateCompareCheck"
    }

    fn validate(
        &self,
        lhs: &Arc<dyn Array>,
        rhs: &Arc<dyn Array>,
        column: [&str; 2],
    ) -> Result<usize, RuleError> {
        // There might be another way to do this
        // and not loop on both array
        let lhs = lhs.as_any().downcast_ref::<Date32Array>().unwrap();
        let rhs = rhs.as_any().downcast_ref::<Date32Array>().unwrap();
        let compare = self.op.get_comparator::<i32>();
        let mut violations: usize = 0;
        for (i, j) in lhs.iter().zip(rhs) {
            match (i, j) {
                (Some(a), Some(b)) => match self.op {
                    CompOperator::Gt => {
                        if !compare(a, b) {
                            violations += 1
                        }
                    }
                    CompOperator::Gte => {
                        if !compare(a, b) {
                            violations += 1
                        }
                    }
                    CompOperator::Eq => {
                        if !compare(a, b) {
                            violations += 1
                        }
                    }
                    CompOperator::Lte => {
                        if !compare(a, b) {
                            violations += 1
                        }
                    }
                    CompOperator::Lt => {
                        if !compare(a, b) {
                            violations += 1
                        }
                    }
                },
                (Some(_), None) => violations += 1,
                (None, Some(_)) => violations += 1,
                (None, None) => {}
            }
        }
        println!("violations: {}", violations);
        Ok(violations)
    }
}
