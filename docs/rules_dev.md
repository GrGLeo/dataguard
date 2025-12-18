PART A: Adding a Generic Rule
Generic rules apply across all column types. Example: NullCheck, UnicityCheck
Step-by-Step Checklist
1. Core Rule Implementation (dataguard-core)
File: crates/dataguard-core/src/rules/generic.rs
- [ ] Create a new struct for your rule (e.g., pub struct MyGenericRule {})
- [ ] Implement constructor: pub fn new() -> Self
- [ ] Implement pub fn name(&self) -> &'static str
- [ ] Implement pub fn validate(&self, array: &dyn Array) -> [return type]
- [ ] Add unit tests in the #[cfg(test)] mod tests section
File: crates/dataguard-core/src/rules/mod.rs
- [ ] Export your new rule

2. Core Column System (dataguard-core)
File: crates/dataguard-core/src/column.rs
- [ ] Add variant to ColumnRule enum
- [ ] Add builder method to appropriate column builder(s):
  - For StringColumnBuilder
  - For NumericColumnBuilder<T> (line ~246-331)

3. Rule Compilation & Execution (dataguard-core)
File: crates/dataguard-core/src/tables/csv_table.rs
For String columns:
- [ ] Add match arm in compile_column_builder() → ColumnType::String:
For Numeric columns (Integer/Float):
- [ ] Add match arm in compile_numeric_rules() function:
For ExecutableColumn storage:
- [ ] Update ExecutableColumn enum in crates/dataguard-core/src/validator/executable_column.rs:
For validation execution:
- [ ] Update validate_string_column() (line ~221-321) if special handling needed
- [ ] Update validate_numeric_column() (line ~323-359) if special handling needed

4. CLI Integration (dataguard-cli)
File: crates/dataguard-cli/src/parser.rs
- [ ] Add variant to Rule enum with serde attributes 
- [ ] Update impl std::fmt::Display for Rule
- [ ] Add validation in validate_column() if needed
File: crates/dataguard-cli/src/constructor.rs
- [ ] Add match arm in appropriate apply_*_rule() function(s)
- [ ] Add unit tests
File: example.toml
- [ ] Add example configuration


PART B: Adding a Type-Specific Rule
Example: Adding a String Rule
1. Core Rule Implementation
File: crates/dataguard-core/src/rules/string.rs
- [ ] Create struct: pub struct MyStringRule { /* fields */ }
- [ ] Implement constructor
- [ ] Implement StringRule trait
- [ ] Add unit tests
File: crates/dataguard-core/src/rules/mod.rs
- [ ] Export: pub use string::MyStringRule;

2. Core Column System
File: crates/dataguard-core/src/column.rs
- [ ] Add variant to ColumnRule enum (line ~59-84)
- [ ] Add builder method to StringColumnBuilder

3. Rule Compilation & Execution
File: crates/dataguard-core/src/tables/csv_table.rs
- [ ] Import your rule at the top
- [ ] Add match arm in compile_column_builder() for ColumnType::String

4. CLI Integration (dataguard-cli)
File: crates/dataguard-cli/src/parser.rs
- [ ] Add variant to Rule enum with serde attributes 
- [ ] Update impl std::fmt::Display for Rule
- [ ] Add validation in validate_column() if needed
File: crates/dataguard-cli/src/constructor.rs
- [ ] Add match arm in appropriate apply_*_rule() function(s)
- [ ] Add unit tests
File: example.toml
- [ ] Add example configuration
