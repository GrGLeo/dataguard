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

impl CompOperator {
    pub fn get_comparator<T: PartialOrd>(&self) -> impl Fn(T, T) -> bool {
        match self {
            CompOperator::Gt => |a, b| a > b,
            CompOperator::Gte => |a, b| a >= b,
            CompOperator::Eq => |a, b| a == b,
            CompOperator::Lte => |a, b| a <= b,
            CompOperator::Lt => |a, b| a < b,
        }
    }
}
