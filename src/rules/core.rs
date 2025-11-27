use pyo3::prelude::*;

/// An enum representing all possible validation rules.
#[pyclass(name = "Rule")]
#[derive(Clone, Debug, PartialEq)] // Add Debug and PartialEq for testing
pub enum Rule {
    StringLength {
        min: Option<usize>,
        max: Option<usize>,
    },
    StringRegex {
        pattern: String,
        flag: Option<String>,
    },
    NumericRange {
        min: Option<f64>,
        max: Option<f64>,
    },
    Monotonicity {
        asc: bool,
    },
}

#[pymethods]
impl Rule {
    #[new]
    fn new() -> Self {
        // A default rule, can be adapted as needed.
        Rule::StringLength {
            min: None,
            max: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_new_default() {
        let default_rule = Rule::new();
        assert_eq!(
            default_rule,
            Rule::StringLength {
                min: None,
                max: None
            }
        );
    }

    #[test]
    fn test_rule_clone() {
        let rule1 = Rule::StringRegex {
            pattern: "abc".to_string(),
            flag: Some("i".to_string()),
        };
        let rule2 = rule1.clone();
        assert_eq!(rule1, rule2);
    }

    #[test]
    fn test_create_string_length_rule() {
        let rule = Rule::StringLength {
            min: Some(5),
            max: Some(10),
        };
        match rule {
            Rule::StringLength { min, max } => {
                assert_eq!(min, Some(5));
                assert_eq!(max, Some(10));
            }
            _ => panic!("Expected StringLength rule"),
        }
    }

    #[test]
    fn test_create_string_regex_rule() {
        let rule = Rule::StringRegex {
            pattern: "^\\d+$".to_string(),
            flag: None,
        };
        match rule {
            Rule::StringRegex { pattern, flag } => {
                assert_eq!(pattern, "^\\d+$".to_string());
                assert_eq!(flag, None);
            }
            _ => panic!("Expected StringRegex rule"),
        }
    }
}
