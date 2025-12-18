┌─────────────────────────────────────────────────────────────────┐
│                    TOML → VALIDATION PIPELINE                   │
└─────────────────────────────────────────────────────────────────┘
1. PARSE PHASE (parser.rs)
   ┌──────────────┐
   │ example.toml │ ──────> Deserialize ──────> Config struct
   └──────────────┘            │                     │
                               │                     ├─ table[]
                               v                     ├─ column[]
                        Validate config              └─ rule[]
                               │
                               v
2. CONSTRUCTION PHASE (constructor.rs)
   ┌──────────────────────────────────────────────────┐
   │  For each column, match on datatype:             │
   │    "string"  → StringColumnBuilder               │
   │    "integer" → IntegerColumnBuilder              │
   │    "float"   → FloatColumnBuilder                │
   └──────────────────────────────────────────────────┘
                               │
                               v
   ┌──────────────────────────────────────────────────┐
   │  Apply rules from TOML to builder:               │
   │    Rule enum (TOML) → Builder methods            │
   │    e.g., IsPositive → builder.is_positive()      │
   └──────────────────────────────────────────────────┘
                               │
                               v
   ┌──────────────────────────────────────────────────┐
   │  Translate to core ColumnRule enum:              │
   │    - StringLength { min, max }                   │
   │    - StringRegex { pattern, flags }              │
   │    - NumericRange { min, max }                   │
   │    - Monotonicity { ascending }                  │
   │    - Unicity                                     │
   └──────────────────────────────────────────────────┘
                               │
                               v
3. COMMIT PHASE (tables/csv_table.rs)
   ┌──────────────────────────────────────────────────┐
   │  CsvTable::commit(builders)                      │
   │    → Creates ExecutableColumn for each builder   │
   └──────────────────────────────────────────────────┘
                               │
                               v
4. COMPILATION PHASE (validator/executable_column.rs)
   ┌──────────────────────────────────────────────────┐
   │  Build optimized validation structures:          │
   │    - Compile regex patterns                      │
   │    - Build HashSets for uniqueness/membership    │
   │    - Prepare numeric comparators                 │
   └──────────────────────────────────────────────────┘
                               │
                               v
5. VALIDATION PHASE (validator/validation.rs)
   ┌──────────────────────────────────────────────────┐
   │  Read CSV → For each row:                        │
   │    For each column:                              │
   │      Execute compiled validators                 │
   │      Collect errors                              │
   └──────────────────────────────────────────────────┘
                               │
                               v
6. REPORTING PHASE (report.rs + formatters/)
   ┌──────────────────────────────────────────────────┐
   │  ValidationResult:                               │
   │    - Pass/Fail status                            │
   │    - Error details with row/column info          │
   │    - Statistics                                  │
   └──────────────────────────────────────────────────┘
                               │
                               v
                        ┌──────────┐
                        │  OUTPUT  │
                        └──────────┘
