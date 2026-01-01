# DataGuard

A data validation tool written in Rust for validating CSV and Parquet files using TOML configuration files.

## Project Structure

This repository is organized as a Cargo workspace with three crates:

### `dataguard-core` - Rust Library
Core validation engine providing data structures, validation rules, and processing logic. Can be used directly in Rust projects.

### `dataguard-py` - Python Bindings
Python bindings for the core library, built with PyO3.

### `dataguard-cli` - Command Line Interface
CLI tool for validating data files using TOML configuration.

## Features

- TOML-based configuration for validation rules
- Available as Rust library, Python package, or CLI tool
- Supported data types: string (Utf8), integer (Int32), float (Float64), date (Date32)
- Validation rules:
  - String: length checks, regex matching, enumeration (isin)
  - Numeric: range validation (min/max)
  - Generic: type checking, uniqueness, null checks
  - Relation: date comparison
  - Statistical: mean, variance, standard deviation for numeric types
- File format support: CSV and Parquet
- Output formats: terminal or JSON
- Watch mode for automatic re-validation (CLI)
- Per-rule validation thresholds

## Installation

### Rust Library (dataguard-core)

```toml
[dependencies]
dataguard-core = { path = "crates/dataguard-core" }
```

### Python Package (dataguard-py)

```bash
cd crates/dataguard-py
pip install .
```

### CLI Tool (dataguard-cli)

```bash
git clone https://github.com/GrGLeo/dataguard.git
cd dataguard
cargo build --release -p dataguard-cli
```

The binary will be available at `./target/release/dataguard-cli`

## Examples

Working examples in both Rust and Python are available in the `examples/` directory. See `examples/README.md` for details.

**Quick run:**
```bash
# Rust example
cargo run --example validate_products

# Python example (after installing dataguard-py)
python examples/validate_products.py
```

## Quick Start with CLI

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

**JSON Output**: Structured validation results

### Example of available validation rules

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

**Note**: A generated Parquet file with columns: **id** (1..512000), **name** (User_1, User_2, ...), and **value** (0.0..999.99, repeating)
is used for testing and stored in `crates/dataguard-core/tests/fixtures`

## Roadmap

- **CSV output**: Add CSV output format alongside JSON and stdout
- **Additional Data Types**: Support for more datatype variants (Int64, LongString, etc.) and time validation
- **Streaming**: Stream processing for large files that cannot fit fully in memory
- **SQL support**: Enable validation on SQL engines (PostgreSQL, Snowflake)
