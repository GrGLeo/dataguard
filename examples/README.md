# DataGuard Examples

This directory contains example code demonstrating how to use DataGuard for data validation in Rust, Python, and via the CLI.

## Example Data

### Small Dataset: `data/products.csv`
Sample e-commerce product data (10 rows) with clean data for testing:
- `id` - Product ID (integer)
- `name` - Product name (string)
- `price` - Product price (float)
- `category` - Product category (string)
- `stock` - Stock quantity (integer)
- `created_date` - Creation date (string, YYYY-MM-DD format)

### Large Dataset: `data/products_large.csv`
Large e-commerce dataset (512k rows) for performance testing:
- `Index` - Row number (integer)
- `Name` - Product name (string)
- `Description` - Product description (string)
- `Brand` - Manufacturer brand (string)
- `Category` - Product category (string)
- `Price` - Product price (integer)
- `Currency` - Price currency code (string)
- `Stock` - Stock quantity (integer)
- `EAN` - European Article Number (13-digit string)
- `Color` - Product color (string)
- `Size` - Product size (string)
- `Availability` - Stock status (string)
- `Internal ID` - Internal identifier (integer)

## Running the Examples

All three examples (Rust, Python, and CLI) use the **same validation rules** and **same dataset** (`products_large.csv` with 512k rows) for consistency.

### Rust Example

Build and run the Rust example:

```bash
# From the repository root (workspace searches for examples automatically)
cargo run --example validate_products

# Or be explicit about which package contains the example
cargo run -p dataguard-core --example validate_products
```

The Rust example demonstrates:
- Creating column builders for different data types (integer, string)
- Adding validation rules (positive values, uniqueness, length checks, membership, exact length)
- Creating a CsvTable and preparing it with columns
- Validating the data and displaying results

**Output:**
```
DataGuard Example - Product Validation

Validating products_large.csv (512k rows)...

Validation Results:
  Table: Products Large Dataset
  Total rows: 512000
  Rules: 24/24 passed

✓ All validation rules passed!
```

### Python Example

First, install the DataGuard Python package:

```bash
# From the repository root
cd crates/dataguard-py
uv venv .venv
source .venv/bin/activate
uv pip install maturin
maturin develop --release
cd ../..
```

Then run the Python example:

```bash
python examples/validate_products.py
```

The Python example demonstrates the same validation logic using the Python API:
- String column validation with length, null, and format checks
- Integer column validation with range, uniqueness, and boundary checks
- String membership validation (currency, availability values)
- Exact length validation (EAN codes)

**Output:**
```
DataGuard Example - Product Validation

Validating products_large.csv (512k rows)...

Validation Results:
  Table: Products Large Dataset
  Total rows: 512000
  Rules: 24/24 passed

✓ All validation rules passed!
```

## Validation Rules Applied

All three examples apply the same 24 validation rules across 9 columns:

1. **Index Column** (integer):
   - Must be unique
   - Cannot be null
   - Must be positive

2. **Name Column** (string):
   - Cannot be null
   - Minimum length of 3 characters
   - Maximum length of 100 characters

3. **Description Column** (string):
   - Minimum length of 1 character

4. **Currency Column** (string):
   - Must be one of: "USD", "EUR", "GBP", "JPY", "CAD"

5. **Price Column** (integer):
   - Must be between 1 and 10,000
   - Must be positive

6. **Stock Column** (integer):
   - Must be non-negative (>= 0)

7. **EAN Column** (string):
   - Must be exactly 13 characters
   - Must be numeric

8. **Availability Column** (string):
   - Must be one of: "in_stock", "out_of_stock", "limited_stock", "backorder", "pre_order", "discontinued"

9. **Internal ID Column** (integer):
   - Must be positive

**Total:** 24 rules across 9 columns, validating 512,000 rows

### CLI Example

DataGuard includes a command-line interface for configuration-based validation.

**Run with the example config:**

```bash
# From the repository root
cargo run -p dataguard-cli -- --config examples/config.toml

# For brief output (summary only)
cargo run -p dataguard-cli -- --config examples/config.toml --brief

# Or after installing the CLI
dataguard-cli --config examples/config.toml
```

**Configuration file:** `config.toml`

The configuration file demonstrates:
- Table definition with CSV file path
- Multiple column type definitions (integer, string)
- Various validation rules:
  - Uniqueness checks (`is_unique`)
  - Null checks (`is_not_null`)
  - Range validation (`between`, `is_positive`, `is_non_negative`)
  - String length constraints (`with_min_length`, `with_max_length`, `is_exact_length`)
  - Membership validation (`is_in`)
  - Format validation (`is_numeric`)

**Expected output:**

```
DataGuard v0.1.0 - Validation Report
====================================
Loading data...
  [1/1] Products Large Dataset

Validating...

Products Large Dataset (512.0K rows) -
PASSED: 24/24 rules valid

===================================
Result: 0 failed, 1 passed
```

## Customizing the Examples

You can modify the examples to:
- Add more validation rules (see the main README for available rules)
- Change threshold values to allow some percentage of errors
- Validate different CSV files
- Add date column validation
- Add cross-column relationship validation
- Test with your own datasets
