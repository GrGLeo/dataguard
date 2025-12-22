use crate::RuleError;

#[derive(Debug, Clone, PartialEq)]
pub enum CompOperator {
    Gt,
    Gte,
    Eq,
    Lte,
    Lt,
}

impl TryFrom<&str> for CompOperator {
    type Error = RuleError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "greater than" | "gt" | ">" => Ok(CompOperator::Gt),
            "greater than or equal" | "gte" | ">=" => Ok(CompOperator::Gte),
            "equal" | "=" => Ok(CompOperator::Eq),
            "lesser than" | "lt" | "<" => Ok(CompOperator::Lt),
            "lesser than or equal" | "lte" | "<=" => Ok(CompOperator::Lte),
            _ => Err(RuleError::IncorrentOperatorError(value.to_string())),
        }
    }
}
