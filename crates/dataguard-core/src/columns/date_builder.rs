use chrono::Datelike;

use crate::{columns::ColumnBuilder, ColumnRule, ColumnType};

#[derive(Debug, Clone)]
pub struct DateColumnBuilder {
    name: String,
    format: String,
    rules: Vec<ColumnRule>,
}

impl ColumnBuilder for DateColumnBuilder {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn column_type(&self) -> ColumnType {
        ColumnType::DateType
    }

    fn rules(&self) -> &[ColumnRule] {
        self.rules.as_slice()
    }

    fn format(&self) -> Option<&str> {
        Some(&self.format)
    }
}

impl DateColumnBuilder {
    pub fn new(name: String, format: String) -> Self {
        Self {
            name,
            format,
            rules: Vec::new(),
        }
    }

    pub fn get_format(&self) -> String {
        self.format.clone()
    }

    /// Add not null constraint
    pub fn is_not_null(&mut self, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::NullCheck { threshold });
        self
    }

    /// Add uniqueness constraint
    pub fn is_unique(&mut self, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::Unicity { threshold });
        self
    }

    /// Set a limit, the date should be before the given date
    pub fn is_before(
        &mut self,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
        threshold: f64,
    ) -> &mut Self {
        self.rules.push(ColumnRule::DateBoundary {
            name: "IsBefore".to_string(),
            threshold,
            after: false,
            year,
            month,
            day,
        });
        self
    }

    /// Set a limit, the date should be after the given date
    pub fn is_after(&mut self, year: usize, month: Option<usize>, day: Option<usize>, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::DateBoundary {
            name: "IsAfter".to_string(),
            threshold,
            after: true,
            year,
            month,
            day,
        });
        self
    }

    /// Infer the date from today, and check that all dates are before today
    pub fn is_not_futur(&mut self, threshold: f64) -> &mut Self {
        let now = chrono::offset::Local::now();
        let year = now.year() as usize;
        let month = Some(now.month() as usize);
        let day = Some(now.day() as usize);
        self.rules.push(ColumnRule::DateBoundary {
            name: "IsNotFutur".to_string(),
            threshold,
            after: false,
            year,
            month,
            day,
        });
        self
    }

    /// Infer the date from today, and check that all dates are after today
    pub fn is_not_past(&mut self, threshold: f64) -> &mut Self {
        let now = chrono::offset::Local::now();
        let year = now.year() as usize;
        let month = Some(now.month() as usize);
        let day = Some(now.day() as usize);
        self.rules.push(ColumnRule::DateBoundary {
            name: "IsNotPast".to_string(),
            threshold,
            after: true,
            year,
            month,
            day,
        });
        self
    }

    pub fn is_weekday(&mut self, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::WeekDay {
            name: "IsWeekday".to_string(),
            threshold,
            is_week: true,
        });
        self
    }

    pub fn is_weekend(&mut self, threshold: f64) -> &mut Self {
        self.rules.push(ColumnRule::WeekDay {
            name: "IsWeekend".to_string(),
            threshold,
            is_week: false,
        });
        self
    }
}
