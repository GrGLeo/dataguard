use crate::parser::Table;
use dataguard_core::{StringColumnBuilder, Validator};

pub fn construct_validator(table: &Table) {
    let path = &table.path;
    for column in &table.column {
        match column.datatype.as_str() {
            "integer" => {}
            "string" => {
                let mut builder = StringColumnBuilder::new(column.name.clone());
                for rule in &column.rule {
                    match rule.name.as_str() {
                        "is_unique" => {
                            builder.is_unique();
                        }
                        "with_min_length" => {
                            if let Some(min) = rule.min_length {
                                builder.with_min_length(min);
                            } else {
                                // TODO: return an error here
                            }
                        }
                        _ => {
                            // TODO: return an error here
                        }
                    }
                }
                let mut v = Validator::new();
                v.commit(vec![Box::new(builder)]).unwrap();
                let _ = v.validate_csv(path, true);
            }
            _ => {}
        }
    }
}
