use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub table_name: String,
    pub total_rows: usize,
    passed: bool,
    pub error_message: Option<String>,
    column_results: HashMap<String, Vec<RuleResult>>,
}

impl ValidationResult {
    pub fn new(table_name: String, total_rows: usize) -> Self {
        Self {
            table_name,
            total_rows,
            passed: true,
            error_message: None,
            column_results: HashMap::new(),
        }
    }

    pub fn add_column_result(&mut self, column_name: String, results: Vec<RuleResult>) {
        self.column_results.insert(column_name, results);
    }

    pub fn add_column_results(&mut self, column_results: HashMap<String, Vec<RuleResult>>) {
        self.column_results = column_results
    }

    pub fn get_column_results(&self) -> HashMap<String, Vec<&RuleResult>> {
        self.column_results
            .iter()
            .map(|(s, v)| (s.clone(), v.iter().collect()))
            .collect()
    }

    pub fn set_failed(&mut self, message: String) {
        self.passed = false;
        self.error_message = Some(message);
    }

    pub fn is_passed(&self) -> bool {
        self.passed
    }
}

#[derive(Debug, Clone)]
pub struct RuleResult {
    pub rule_name: String,
    pub error_count: usize,
    pub error_percentage: f64,
}

impl RuleResult {
    pub fn new(rule_name: String, error_count: usize, error_percentage: f64) -> Self {
        Self {
            rule_name,
            error_count,
            error_percentage,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_validaton_result_instanciation() {
        let table_name = "products".to_string();
        let total_rows = 20_000_000usize;
        let validation = ValidationResult::new(table_name.clone(), total_rows);

        assert_eq!(validation.passed, true);
        assert_eq!(validation.table_name, table_name);
        assert_eq!(validation.total_rows, total_rows);
    }

    #[test]
    fn test_validaton_result_failed() {
        let table_name = "products".to_string();
        let total_rows = 20_000_000usize;
        let mut validation = ValidationResult::new(table_name.clone(), total_rows);

        assert_eq!(validation.passed, true);
        validation.set_failed(String::from("Failed"));
        assert_eq!(validation.passed, false);
        assert!(validation.error_message.is_some());
    }
}
