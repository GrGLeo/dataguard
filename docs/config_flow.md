# TOML → Validation Pipeline

This document describes the complete flow from TOML configuration to validation results in DataGuard.

## Architecture Overview

```mermaid
flowchart TD
    A[example.toml] --> B[1. Parse Phase]
    B --> C[2. Construction Phase]
    C --> D[3. Table Preparation Phase]
    D --> E[4. Compilation Phase]
    E --> F[5. Validation Phase]
    F --> G[6. Reporting Phase]
    G --> H[Output]
```

## Detailed Flow

### 1. Parse Phase
**Location:** `crates/dataguard-cli/src/parser.rs`

```mermaid
flowchart LR
    A[example.toml] --> B[Deserialize]
    B --> C{Validate Config}
    C -->|Valid| D[Config Struct]
    C -->|Invalid| E[ConfigError]

    D --> F[table: Vec&lt;ConfigTable&gt;]
    F --> G[column: Vec&lt;Column&gt;]
    G --> H[rule: Vec&lt;Rule&gt;]
```

**Responsibilities:**
- Deserialize TOML into strongly-typed Rust structs
- Validate config rules (e.g., `min < max`, file paths exist)
- Parse rule enums (IsUnique, WithLengthBetween, Between, etc.)

**Key Types:**
- `Config` - Root configuration
- `ConfigTable` - Table definition with name and path
- `Column` - Column definition with name, datatype, and rules
- `Rule` - Enum of all available validation rules

### 2. Construction Phase
**Location:** `crates/dataguard-cli/src/constructor.rs`

```mermaid
flowchart TD
    A[Config] --> B{For each Column}
    B --> C{Match datatype}

    C -->|string| D[StringColumnBuilder]
    C -->|integer| E[NumericColumnBuilder&lt;i64&gt;]
    C -->|float| F[NumericColumnBuilder&lt;f64&gt;]
    C -->|date| G[DateColumnBuilder]

    D --> H[Apply String Rules]
    E --> I[Apply Integer Rules]
    F --> J[Apply Float Rules]
    G --> K[Apply Date Rules]

    H --> L[ColumnRule Enums]
    I --> L
    J --> L
    K --> L

    L --> M[Box&lt;dyn ColumnBuilder&gt;]
```

**Responsibilities:**
- Match TOML datatypes to appropriate builder types
- Apply rule methods to builders (e.g., `builder.is_positive()`)
- Translate TOML `Rule` enum to core `ColumnRule` enum

**Rule Translation Examples:**
- `Rule::IsPositive` → `builder.is_positive()` → `ColumnRule::NumericRange { min: Some(0), max: None }`
- `Rule::WithLengthBetween { min, max }` → `builder.with_length_between(min, max)` → `ColumnRule::StringLength { min, max }`
- `Rule::IsUnique` → `builder.is_unique()` → `ColumnRule::Unicity`

### 3. Table Preparation Phase
**Location:** `crates/dataguard-core/src/tables/csv_table.rs`

```mermaid
flowchart LR
    A[Vec&lt;Box&lt;dyn ColumnBuilder&gt;&gt;] --> B[CsvTable::prepare]
    B --> C{For each Builder}
    C --> D[compile_column]
    D --> E[ExecutableColumn]
    E --> F[Store in columns Vec]
```

**Responsibilities:**
- Create `CsvTable` instance with path and name
- Call `prepare()` with column builders
- Internally calls the compiler for each builder

### 4. Compilation Phase
**Location:** `crates/dataguard-core/src/compiler/mod.rs`

```mermaid
flowchart TD
    A[ColumnBuilder] --> B{Column Type}

    B -->|String| C[compile_string_rules]
    B -->|Integer| D[compile_numeric_rules&lt;i64&gt;]
    B -->|Float| E[compile_numeric_rules&lt;f64&gt;]
    B -->|Date| F[compile_date_rules]

    C --> G[Vec&lt;Box&lt;dyn StringRule&gt;&gt;]
    D --> H[Vec&lt;Box&lt;dyn NumericRule&gt;&gt;]
    E --> I[Vec&lt;Box&lt;dyn NumericRule&gt;&gt;]
    F --> J[Vec&lt;Box&lt;dyn DateRule&gt;&gt;]

    G --> K[StringLengthCheck]
    G --> L[RegexMatch]
    G --> M[IsInCheck]

    H --> N[Range]
    H --> O[Monotonicity]

    K --> P[ExecutableColumn::String]
    L --> P
    M --> P
    N --> Q[ExecutableColumn::Integer/Float]
    O --> Q
    J --> R[ExecutableColumn::Date]
```

**Responsibilities:**
- Compile regex patterns (for StringRegex rules)
- Build HashSets for membership checks (IsIn rule)
- Prepare numeric comparators (Range, Monotonicity)
- Separate domain rules from meta-rules (Unicity, NullCheck)
- Add TypeCheck for CSV tables (string → typed conversion)

**Key Optimizations:**
- Regex compiled once, not per-row
- HashSets built once for O(1) membership checks
- Trait objects enable polymorphic validation

### 5. Validation Phase
**Location:** `crates/dataguard-core/src/engine/validaton_engine.rs`

```mermaid
flowchart TD
    A[CSV File] --> B[Read as RecordBatches]
    B --> C[ValidationEngine]

    C --> D{For each Batch parallel}
    D --> E{For each Column}

    E --> F{Column Type}

    F -->|String| G[validate_string_column]
    F -->|Integer| H[validate_numeric_column&lt;Int64Type&gt;]
    F -->|Float| I[validate_numeric_column&lt;Float64Type&gt;]
    F -->|Date| J[validate_date_column]

    G --> K[1. TypeCheck optional]
    H --> K
    I --> K
    J --> K

    K --> L[2. NullCheck optional]
    L --> M[3. Domain Rules]
    M --> N[4. UnicityCheck local]

    N --> O[ResultAccumulator]
    O --> P[UnicityAccumulator finalize]
    P --> Q[ValidationResult]
```

**Responsibilities:**
- Read CSV into Arrow RecordBatches
- Validate batches in parallel using Rayon
- Execute validation in strict order:
  1. **TypeCheck** - Convert strings to typed arrays (CSV only)
  2. **NullCheck** - Count null values
  3. **Domain Rules** - Execute type-specific validators
  4. **Unicity** - Build local hash sets per batch
- Finalize unicity across all batches (detect duplicates)
- Accumulate errors in thread-safe ResultAccumulator

**Key Features:**
- Parallel batch processing for performance
- Arrow arrays for zero-copy columnar access
- Atomic error counting across threads
- Global unicity tracking via UnicityAccumulator

### 6. Reporting Phase
**Location:** `crates/dataguard-reports/src/`

```mermaid
flowchart LR
    A[ValidationResult] --> B{Reporter Type}

    B -->|StdOut| C[StdOutFormatter]
    B -->|JSON| D[JsonFormatter]

    C --> E[Console Output]
    D --> F[JSON File]

    A --> G[Column Results]
    G --> H[Rule Error Counts]
    H --> I[Statistics]

    I --> J[Total Rows]
    I --> K[Total Errors]
    I --> L[Pass/Fail Status]
```

**Responsibilities:**
- Format validation results for output
- Support multiple output formats (stdout, JSON)
- Provide detailed error statistics per column/rule
- Show overall pass/fail status

**Output Includes:**
- Table name and file path
- Total rows processed
- Per-column error counts
- Per-rule error counts
- Overall validation status

## Key Data Structures

### Rule Hierarchy

```mermaid
classDiagram
    class Rule {
        <<TOML Layer>>
        +IsUnique
        +WithLengthBetween
        +Between
        +IsPositive
        +IsIncreasing
    }

    class ColumnRule {
        <<Core Layer>>
        +StringLength
        +StringRegex
        +NumericRange
        +Monotonicity
        +Unicity
    }

    class ExecutableColumn {
        <<Runtime Layer>>
        +String
        +Integer
        +Float
        +Date
    }

    class Validators {
        <<Implementation>>
        +StringLengthCheck
        +RegexMatch
        +Range
        +Monotonicity
        +UnicityCheck
    }

    Rule --> ColumnRule : constructor.rs
    ColumnRule --> ExecutableColumn : compiler/mod.rs
    ExecutableColumn --> Validators : contains trait objects
```

## File References

| Phase | Module | Key Files |
|-------|--------|-----------|
| 1. Parse | `dataguard-cli` | `parser.rs:151` - `parse_config()` |
| 2. Construction | `dataguard-cli` | `constructor.rs:279` - `construct_csv_table()` |
| 3. Preparation | `dataguard-core` | `tables/csv_table.rs` - `CsvTable::prepare()` |
| 4. Compilation | `dataguard-core` | `compiler/mod.rs:181` - `compile_column()` |
| 5. Validation | `dataguard-core` | `engine/validaton_engine.rs:34` - `validate_batches()` |
| 6. Reporting | `dataguard-reports` | `formatters/` - `StdOutFormatter`, `JsonFormatter` |

## Supported Datatypes

| TOML Type | Builder | Arrow Type | Supported Rules |
|-----------|---------|------------|-----------------|
| `string` | `StringColumnBuilder` | `Utf8` | Length, Regex, Membership, IsUnique, IsNotNull, Alpha, Numeric, Email, URL, UUID |
| `integer` | `NumericColumnBuilder<i64>` | `Int64` | Range, Min, Max, Positive/Negative, Monotonicity, IsUnique, IsNotNull |
| `float` | `NumericColumnBuilder<f64>` | `Float64` | Range, Min, Max, Positive/Negative, Monotonicity, IsUnique |
| `date` | `DateColumnBuilder` | `Date32` | Before, After, NotPast, NotFutur, IsUnique, IsNotNull |
