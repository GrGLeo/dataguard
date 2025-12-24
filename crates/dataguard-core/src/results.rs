use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub table_name: String,
    pub total_rows: usize,
    pub error_message: Option<String>,
    column_values: HashMap<String, usize>,
    column_results: HashMap<String, Vec<RuleResult>>,
    relation_results: HashMap<String, Vec<RuleResult>>,
}

impl ValidationResult {
    pub fn new(table_name: String, total_rows: usize) -> Self {
        Self {
            table_name,
            total_rows,
            error_message: None,
            column_values: HashMap::new(),
            column_results: HashMap::new(),
            relation_results: HashMap::new(),
        }
    }

    pub fn add_column_values(&mut self, column_name: String, values: usize) {
        self.column_values.insert(column_name, values);
    }

    pub fn add_columns_values(&mut self, column_values: HashMap<String, usize>) {
        self.column_values = column_values
    }

    pub fn add_column_result(&mut self, column_name: String, results: Vec<RuleResult>) {
        self.column_results.insert(column_name, results);
    }

    pub fn add_column_results(&mut self, column_results: HashMap<String, Vec<RuleResult>>) {
        self.column_results = column_results
    }

    pub fn add_relation_result(&mut self, relation_name: String, results: Vec<RuleResult>) {
        self.relation_results.insert(relation_name, results);
    }

    pub fn add_relation_results(&mut self, relation_results: HashMap<String, Vec<RuleResult>>) {
        self.relation_results = relation_results
    }

    pub fn get_column_results(&self) -> HashMap<String, Vec<&RuleResult>> {
        self.column_results
            .iter()
            .map(|(s, v)| (s.clone(), v.iter().collect()))
            .collect()
    }

    pub fn get_relation_results(&self) -> HashMap<String, Vec<&RuleResult>> {
        self.relation_results
            .iter()
            .map(|(s, v)| (s.clone(), v.iter().collect()))
            .collect()
    }

    /// Returns the number of passed rules and the total number of rules executed.
    ///
    /// The return value is a tuple `(passed_count, total_count)`:
    /// - The first element is the sum of all rules where `status` was `true`.
    /// - The second element is the total count of all rules across all columns.
    ///
    /// # Panics
    ///
    /// This function will panic if the number of rules exceeds [`u8::MAX`] (255),
    /// as the counts are cast or collected into `u8`.
    /// This allow for 255 total number of rules per Table
    pub fn is_passed(&self) -> (u8, u8) {
        let passed = self
            .column_results
            .values()
            .flat_map(|rules| rules.iter().map(|rule| rule.status as u8))
            .collect::<Vec<u8>>();

        (passed.iter().sum(), passed.len() as u8)
    }
}

#[derive(Debug, Clone)]
pub struct RuleResult {
    pub rule_name: String,
    pub error_count: usize,
    pub error_percentage: f64,
    pub error_message: Option<String>,
    pub status: bool,
}

impl RuleResult {
    pub fn new(
        rule_name: String,
        error_count: usize,
        error_percentage: f64,
        error_message: Option<String>,
        status: bool,
    ) -> Self {
        Self {
            rule_name,
            error_count,
            error_percentage,
            error_message,
            status,
        }
    }

    pub fn set_error_message(&mut self, error_message: String) {
        self.error_message = Some(error_message)
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

        assert_eq!(validation.table_name, table_name);
        assert_eq!(validation.total_rows, total_rows);
    }
}
