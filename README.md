# DataGuard

DataGuard is a high-performance data validation CLI tool written in Rust. It provides a flexible and efficient way to define and apply validation rules to CSV files using TOML configuration files, ensuring data quality and integrity.

## Features

- **TOML-based Configuration**: Define validation rules in a simple, declarative format
- **Supported Data Types**:  string(Utf8), integer(Int32), float(Float64), and date(Date32) columns
- **Comprehensive Validation Rules**:
  - String: length checks, regex matching, enumeration (isin)
  - Numeric: range validation (min/max)
  - Generic: type checking, uniqueness, null checks
  - Relation: date comparaison
- **Flexible Output**: Human-readable table reports or JSON format
- **Watch Mode**: Automatic re-validation on file changes
- **Threshold**: Set per rule validation threshold
- **Performance**: Built with parallel processing and optimized validation logic

## Installation

### From Source

```bash
git clone https://github.com/GrGLeo/dataguard.git
cd dataguard
cargo build --release -p dataguard-cli
```

The binary will be available at `./target/release/dataguard-cli`

## Quick Start

1. Create a TOML configuration file (`validation.toml`):

```toml
[[table]]
name = "products"
path = "data/products.csv"

  [[table.column]]
  name = "Id"
  datatype = "integer"

    [[table.column.rule]]
    name = "min"
    min = 1

  [[table.column]]
  name = "Name"
  datatype = "string"

    [[table.column.rule]]
    name = "with_min_length"
    min_length = 3

    [[table.column.rule]]
    name = "is_unique"
```

2. Run validation:

```bash
dataguard-cli --config validation.toml
```

## Usage

### Basic Validation

```bash
# Validate with table output
dataguard-cli --config validation.toml --output stdout

# Validate with JSON output
dataguard-cli --config validation.toml --output json --path results/

# Brief report (PASS/FAIL per table)
dataguard-cli --config validation.toml --brief
```

### Watch Mode

Automatically re-run validation when files change:

```bash
dataguard-cli --config validation.toml --watch
```

### Output Formats

**Standard Output** (default):
```
DataGuard v0.1.0 - Validation Report
====================================
Loading data...
  [1/2] products_large
  [2/2] customers_medium

Validating...

products_large (20.0M rows) -
FAILED: 2/3 rules valid
  Column results:
    Name:
      StringLengthCheck .......... 249.2K (01.25%) PASS
      TypeCheck ..................      0 (00.00%) PASS
      UnicityCheck ...............  19.1M (95.48%) FAIL

  Relation results:
    Shipped-date | Received-date:
      LessThan....................      0 (00.00%) PASS


customers_medium (2.0M rows) -
PASSED: 2/2 rules valid
  Column results:
    Index:
      NumericRange ...............      0 (00.00%) PASS
      TypeCheck ..................      0 (00.00%) PASS

===================================
Result: 1 failed, 1 passed
```

**JSON Output**: Structured validation results with detailed error information

### Available Validation Rules

**String Rules**:
- `with_min_length`: Minimum string length
- `with_max_length`: Maximum string length
- `with_regex`: Pattern matching
- `isin`: Value must be in a specified set

**Numeric Rules** (integer/float):
- `min`: Minimum value
- `max`: Maximum value
- `is_non_negative`: Value must be >= 0

**Generic Rules**:
- `is_not_null`: Column cannot contain null values
- `is_unique`: All values must be unique

## CLI Options

```
Options:
  -c, --config <FILE>    Path to TOML configuration file
  -o, --output <FORMAT>  Output format: stdout or json [default: stdout]
  -p, --path <PATH>      Path for JSON output (file or directory)
  -b, --brief            Enable brief report (PASS/FAIL per table)
  -d, --debug            Enable debug mode with stack traces
  -w, --watch            Watch mode: auto-validate on file changes
  -h, --help             Print help
  -V, --version          Print version
```

## Development

### Running Tests

```bash
# Run all tests
cargo test
```

**Note**: A generated parquet with columns: id: 1..512000 name: User_1, User_2.... and value: 0.0, .. 999.99, 0.0.. is used for testing and stored
in `crates/dataguard-core/tests/fixtures`

## Roadmap

### Planned Features

- **Statistical Validation Rules**: Z-score outlier detection, IQR-based validation, percentile checks
- **CSV output**: Adding along side json and stdout a csv output.
- **Additional Data Types**: Support for more variant of datatype (Int64, LongString...), time validation.
- **Loading Performance**: Reducing csv loading time, and starting validation of present batch, while loading next batch
