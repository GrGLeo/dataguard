use std::collections::HashMap;

use crate::rules::Rule;

pub type RuleMap =  HashMap<String, Vec<Box<dyn Rule + Send + Sync>>>;
