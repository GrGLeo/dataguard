use arrow_array::Array;

use crate::{utils::operator::CompOperator, RuleError};

pub trait RelationRule {
    fn name(&self) -> &'static str;
    fn validate(
        &self,
        lhs: Box<dyn Array>,
        rhs: Box<dyn Array>,
        columns: [String; 2],
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
        lhs: Box<dyn Array>,
        rhs: Box<dyn Array>,
        column: [String; 2],
    ) -> Result<usize, RuleError> {
        todo!();
    }
}
