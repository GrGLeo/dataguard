use crate::{columns::ColumnBuilder, ColumnRule, ColumnType};

#[derive(Debug, Clone)]
pub struct DateColumnBuilder {
    name: String,
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
}

impl DateColumnBuilder {
    pub fn new(name: String) -> Self {
        Self {
            name,
            rules: Vec::new(),
        }
    }

    /// Add not null constraint
    pub fn is_not_null(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::NullCheck);
        self
    }

    /// Add uniqueness constraint
    pub fn is_unique(&mut self) -> &mut Self {
        self.rules.push(ColumnRule::Unicity);
        self
    }

    /// Set a limit, the date should be before the given date
    pub fn is_before(
        &mut self,
        year: usize,
        month: Option<usize>,
        day: Option<usize>,
    ) -> &mut Self {
        self.rules.push(ColumnRule::DateBoundary {
            after: false,
            year,
            month,
            day,
        });
        self
    }

    /// Set a limit, the date should be after the given date
    pub fn is_after(&mut self, year: usize, month: Option<usize>, day: Option<usize>) -> &mut Self {
        self.rules.push(ColumnRule::DateBoundary {
            after: true,
            year,
            month,
            day,
        });
        self
    }
}
