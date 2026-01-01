# DataGuard Examples

This directory contains example code demonstrating how to use DataGuard for data validation in Rust, Python, and via the CLI.

## Example Data

### Large Dataset: `data/products_large.csv`
Large e-commerce dataset (2M rows) for testing. The dataset is an example of [products datasets](https://www.datablist.com/learn/csv/download-sample-csv-files#products-dataset).

**Columns:**
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

## Validation Rules

All three examples (Rust, Python, and CLI) apply the **same 24 validation rules** across 9 columns:

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

## Running the Examples

### Rust Example

**Run the example:**

```bash
cargo run --example validate_products
```

**What it demonstrates:**
- Creating column builders for different data types (integer, string)
- String column validation with length, null, and format checks
- Integer column validation with range, uniqueness, and boundary checks
- String membership validation (currency, availability values)
- Exact length validation (EAN codes)
- Creating a CsvTable and preparing it with columns
- Validating the data and displaying results

### Python Example

**Setup:**

```bash
# From the repository root
cd crates/dataguard-py
uv venv .venv
source .venv/bin/activate
uv pip install maturin
maturin develop --release
cd ../..
```

**Run the example:**

```bash
python examples/validate_products.py
```

**What it demonstrates:**
- Using the Python API to define column builders
- Applying the same validation rules as the Rust example
- Programmatic validation with Python-native error handling

### CLI Example

**Run the example:**

```bash
# From the repository root
cargo run -p dataguard-cli -- --config examples/config.toml

# For brief output (summary only)
cargo run -p dataguard-cli -- --config examples/config.toml --brief

# Or after installing the CLI
dataguard-cli --config examples/config.toml
```

**What it demonstrates:**
- Configuration-based validation using TOML files
- Table definition with CSV file path
- Multiple column type definitions (integer, string)
- Declarative validation rules:
  - Uniqueness checks (`is_unique`)
  - Null checks (`is_not_null`)
  - Range validation (`between`, `is_positive`, `is_non_negative`)
  - String length constraints (`with_min_length`, `with_max_length`, `is_exact_length`)
  - Membership validation (`is_in`)
  - Format validation (`is_numeric`)

## Expected Output

All three examples validate the same dataset with the same rules and produce similar output:

**Rust & Python:**
```
DataGuard Example - Product Validation

Validating products_large.csv (512k rows)...

Validation Results:
  Table: Products Large Dataset
  Total rows: 512000
  Rules: 24/24 passed

✓ All validation rules passed!
```

**CLI:**
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
