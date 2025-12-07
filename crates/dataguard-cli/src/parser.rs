use serde::Deserialize;
use toml::Value;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub table: Vec<Table>,
}

#[derive(Debug, Deserialize)]
pub struct Table {
    pub name: String,
    pub path: String,
    pub column: Vec<Column>,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    pub datatype: String,
    pub rule: Vec<Rule>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Rule {
    pub name: String,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub length: Option<usize>,
    pub members: Option<Vec<String>>,
    pub pattern: Option<String>,
    pub flag: Option<String>,
    pub min: Option<Value>,
    pub max: Option<Value>,
}
