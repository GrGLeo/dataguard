use crate::{columns::TableConstraint, utils::operator::CompOperator};

pub struct RelationBuilder {
    pub names: [String; 2],
    pub rules: Vec<TableConstraint>,
}

impl RelationBuilder {
    pub fn new(names: [String; 2]) -> Self {
        Self {
            names,
            rules: Vec::new(),
        }
    }

    pub fn rules(&self) -> &[TableConstraint] {
        &self.rules
    }

    pub fn names(&self) -> [String; 2] {
        [self.names[0].clone(), self.names[1].clone()]
    }

    pub fn date_comparaison(&mut self, op: CompOperator, threshold: f64) -> &mut Self {
        self.rules
            .push(TableConstraint::DateComparaison { op, threshold });
        self
    }

    pub fn numeric_comparaison(&mut self, op: CompOperator, threshold: f64) -> &mut Self {
        self.rules
            .push(TableConstraint::NumericComparaison { op, threshold });
        self
    }
}
