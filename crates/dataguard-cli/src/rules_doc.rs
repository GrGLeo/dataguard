/// Print comprehensive documentation of all available validation rules
pub fn print_rules_documentation() {
    println!("{}", rules_documentation());
}
fn rules_documentation() -> String {
    format!(
        r#"
╔══════════════════════════════════════════════════════════════════════════════╗
║                        DATAGUARD VALIDATION RULES                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
{}
{}
{}
{}
"#,
        null_handling_section(),
        generic_rules_section(),
        string_rules_section(),
        numeric_rules_section()
    )
}

fn null_handling_section() -> String {
    r#"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 NULL HANDLING POLICY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRINCIPLE:
  Domain rules validate domain constraints. Null checking validates existence.
BEHAVIOR:
  • Domain Rules (range, length, regex, etc.):
    → Skip null values - only validate non-null data
    → Nulls are NOT counted as errors

  • Null Check (is_not_null):
    → Explicitly counts null values as errors
    → Use when null values are not allowed

  • Uniqueness (is_unique):
    → Ignores null values (multiple nulls allowed)
    → Follows SQL NULL semantics: NULL != NULL

  • Monotonicity (is_increasing/is_decreasing):
    → Currently ignores nulls (see MONOTONICITY_NULL_HANDLING.md)
    → Future behavior may change
EXAMPLES:
  • between(0, 100):
    → Validates: [50, null, 75, null, 90]
    → Errors: 0 (nulls are skipped)

  • is_not_null().between(0, 100):
    → Validates: [50, null, 75, null, 90]
    → Errors: 2 (2 nulls) + any range violations

  • is_unique():
    → Validates: ["a", null, "b", null, "c"]
    → Errors: 0 (nulls don't violate uniqueness)
"#
    .to_string()
}

fn generic_rules_section() -> String {
    r#"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 GENERIC RULES (All Data Types)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_unique                                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Enforces uniqueness constraint - no duplicate values allowed   │
│ Null Handling: Ignores nulls (multiple nulls are allowed)                   │
│ Use Case: Email addresses, user IDs, product SKUs                           │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_unique"                                                         │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_not_null                                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Requires non-null values - null values are errors              │
│ Null Handling: Counts null values as errors                                 │
│ Use Case: Required fields, mandatory columns                                │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_not_null"                                                       │
└─────────────────────────────────────────────────────────────────────────────┘
"#
    .to_string()
}

fn string_rules_section() -> String {
    r#"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 STRING RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌─────────────────────────────────────────────────────────────────────────────┐
│ with_length_between                                                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String length must be in range [min, max] (inclusive)          │
│ Null Handling: Skips nulls                                                   │
│ Parameters: min_length (usize), max_length (usize)                           │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "with_length_between"                                               │
│   min_length = 5                                                             │
│   max_length = 50                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ with_min_length                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String length must be >= min                                    │
│ Null Handling: Skips nulls                                                   │
│ Parameters: min_length (usize)                                               │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "with_min_length"                                                   │
│   min_length = 3                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ with_max_length                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String length must be <= max                                    │
│ Null Handling: Skips nulls                                                   │
│ Parameters: max_length (usize)                                               │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "with_max_length"                                                   │
│   max_length = 100                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_exact_length                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String must have exact length                                   │
│ Null Handling: Skips nulls                                                   │
│ Parameters: length (usize)                                                   │
│ Use Case: Fixed-width fields, country codes (ISO2: "US", "FR")              │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_exact_length"                                                   │
│   length = 2                                                                 │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_in                                                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String must be one of the allowed values                        │
│ Null Handling: Skips nulls                                                   │
│ Parameters: members (array of strings)                                       │
│ Use Case: Enums, categories, status values                                   │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_in"                                                             │
│   members = ["pending", "approved", "rejected"]                              │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ with_regex                                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String must match regex pattern                                 │
│ Null Handling: Skips nulls                                                   │
│ Parameters: pattern (string), flag (optional: "i" for case-insensitive)     │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "with_regex"                                                        │
│   pattern = "^[A-Z]{2}\\d{4}$"  # Two letters, four digits                  │
│   flag = "i"                     # Optional: case-insensitive                │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_numeric, is_alpha, is_alphanumeric                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String character type validation                                │
│   • is_numeric: Only digits (0-9)                                            │
│   • is_alpha: Only letters (a-zA-Z)                                          │
│   • is_alphanumeric: Only letters and digits                                 │
│ Null Handling: Skips nulls                                                   │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_numeric"                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_uppercase, is_lowercase                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: String case validation                                          │
│   • is_uppercase: All letters must be uppercase                             │
│   • is_lowercase: All letters must be lowercase                             │
│ Null Handling: Skips nulls                                                   │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_uppercase"                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_email, is_url, is_uuid                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Format validation for common data types                         │
│   • is_email: Valid email format (user@domain.com)                          │
│   • is_url: Valid URL format (https://example.com)                          │
│   • is_uuid: Valid UUID format (550e8400-e29b-41d4-a716-446655440000)       │
│ Null Handling: Skips nulls                                                   │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_email"                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
"#
    .to_string()
}
fn numeric_rules_section() -> String {
    r#"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 NUMERIC RULES (Integer & Float)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┌─────────────────────────────────────────────────────────────────────────────┐
│ between                                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Value must be in range [min, max] (inclusive)                  │
│ Null Handling: Skips nulls                                                   │
│ Parameters: min (number), max (number)                                       │
│ Applies To: Integer, Float                                                   │
│                                                                              │
│ TOML Example (Integer):                                                      │
│   [[table.column.rule]]                                                      │
│   name = "between"                                                           │
│   min = 0                                                                    │
│   max = 100                                                                  │
│                                                                              │
│ TOML Example (Float):                                                        │
│   [[table.column.rule]]                                                      │
│   name = "between"                                                           │
│   min = 0.0                                                                  │
│   max = 1.0                                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ min, max                                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Value bounds                                                    │
│   • min: Value must be >= min                                               │
│   • max: Value must be <= max                                               │
│ Null Handling: Skips nulls                                                   │
│ Parameters: min (number) or max (number)                                     │
│ Applies To: Integer, Float                                                   │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "min"                                                               │
│   min = 0                                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_positive, is_negative                                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Sign validation                                                 │
│   • is_positive: Value > 0                                                  │
│   • is_negative: Value < 0                                                  │
│ Null Handling: Skips nulls                                                   │
│ Applies To: Integer, Float                                                   │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_positive"                                                       │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_non_positive, is_non_negative                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Sign validation (inclusive)                                     │
│   • is_non_positive: Value <= 0                                             │
│   • is_non_negative: Value >= 0                                             │
│ Null Handling: Skips nulls                                                   │
│ Applies To: Integer, Float                                                   │
│ Use Case: Allows zero in range                                               │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_non_negative"                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────────────┐
│ is_increasing, is_decreasing                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ Description: Monotonicity validation (checks order of values)                │
│   • is_increasing: Each value >= previous value                             │
│   • is_decreasing: Each value <= previous value                             │
│ Null Handling: Currently ignores nulls (behavior may change)                │
│ Applies To: Integer, Float                                                   │
│ Use Case: Timestamps, sequential IDs, sorted data                            │
│ Note: See MONOTONICITY_NULL_HANDLING.md for discussion on null behavior     │
│                                                                              │
│ TOML Example:                                                                │
│   [[table.column.rule]]                                                      │
│   name = "is_increasing"                                                     │
└─────────────────────────────────────────────────────────────────────────────┘
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 USAGE NOTES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Rules can be combined: Use multiple rules on same column
  • Order matters: Rules are evaluated in order specified in config
  • Null handling: Most rules skip nulls - use is_not_null to forbid them
  • Data types: String rules only work on String columns
               Numeric rules work on both Integer and Float columns

  For more information:
    dataguard --help          # General CLI help
    https://github.com/...    # Full documentation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"#
    .to_string()
}
