# DataGuard

DataGuard is a high-performance data validation library, written in Rust with Python bindings. It provides a flexible and efficient way to define and apply rules to your data, ensuring data quality and integrity.

## Features

- Read and validate CSV files.
- Define and apply validation rules to string columns via the Python API.
- High-performance validation, significantly faster than traditional methods.

## Performance

DataGuard is built for speed. Its significant performance improvements compared to traditional methods are a result of two key optimizations: **parallel CSV reading** and **optimized data validation logic**. In our benchmarks, we've observed:

- **2.21x faster** for a 200 thousand row CSV.
- **7.19x faster** for a 2 million row CSV.
- **8.35x faster** for a 20 million row CSV.

## Usage

The Python API provides a simple way to interact with the Rust backend and define your validation rules.

```python
import dataguard

# 1. Create a Validator instance
validator = dataguard.Validator()

# 2. Define rules for your columns
# For a string column named 'product_id':
product_id_col = dataguard.string_column("product_id") \
                           .with_min_length(5) \
                           .with_max_length(10) \
                           .build()

# For another string column named 'description' with a regex and min length:
description_col = dataguard.string_column("description") \
                            .with_regex("^[a-zA-Z0-9 ]+$") \
                            .with_min_length(10) \
                            .build()

# 3. Commit the column rules to the validator
validator.commit([product_id_col, description_col])

# 4. Validate a CSV file
# 'your_data.csv' should be replaced with the actual path to your CSV file
error_count = validator.validate_csv("your_data.csv", print_report=True)

if error_count == 0:
    print("Validation successful! No errors found.")
else:
    print(f"Validation finished with {error_count} errors.")

# You can also inspect the configured rules:
# configured_rules = validator.get_rules()
# print(configured_rules)
```

With `print_report` set to True the `validate_csv` method also output a table report:
```
+----------+-------------------+-------------+---------+
| Column   | Rule              | Error Count | % Error |
+----------+-------------------+-------------+---------+
| Category | RegexMatch        | 1941312     | 97.07%  |
+----------+-------------------+-------------+---------+
| Category | TypeCheck         | 0           | 0.00%   |
+----------+-------------------+-------------+---------+
| Currency | StringLengthCheck | 1           | 0.00%   |
+----------+-------------------+-------------+---------+
| Currency | TypeCheck         | 0           | 0.00%   |
+----------+-------------------+-------------+---------+
```

## Roadmap

This project is still in its early phase. Here's what we have planned for the near future:

- **Integer Column Rules**:
  - Enumeration checks

- **String Column Rules**:
  - `isin`: check if all value in the array is in a given set of words (e.g., "SEND", "ORDERED", "RECEIVED").

- **Declarative Rules**:
  - We are working on a `TOML` based configuration file to declare the rules that should be applied to a validation. This will allow you to define your validation rules in a simple and readable format.
