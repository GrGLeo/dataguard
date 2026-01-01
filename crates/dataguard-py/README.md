# DataGuard Python Bindings

Python bindings for DataGuard, providing data validation for CSV and Parquet files.

## Installation

```bash
pip install dataguard
```

## Features

- CSV and Parquet file validation
- Fluent API for defining validation rules
- String, numeric, and date column support
- Cross-column relationship validation
- Configurable error thresholds

## Quick Start

```python
from dataguard import CsvTable, string_column, integer_column, date_column

# Define validation rules
name = string_column("name").with_min_length(2).is_not_null()
age = integer_column("age").between(0, 120)
email = string_column("email").is_email()
birth_date = date_column("birth_date", "%Y-%m-%d").is_not_futur()

# Create and prepare table
table = CsvTable("users.csv", "users_table")
table.prepare([name, age, email, birth_date])

# Validate
result = table.validate()
print(f"Validated {result['total_rows']} rows")
print(f"Passed {result['passed'][0]}/{result['passed'][1]} rules")
```

## Column Types

### String Columns

```python
from dataguard import string_column

col = (string_column("email")
    .with_min_length(5)
    .with_max_length(100)
    .is_email()
    .is_not_null())
```

Available string rules:
- `with_min_length(min, threshold=0.0)` - Minimum length
- `with_max_length(max, threshold=0.0)` - Maximum length
- `with_length_between(min, max, threshold=0.0)` - Length range
- `is_exact_length(len, threshold=0.0)` - Exact length
- `is_in(values, threshold=0.0)` - Value must be in list
- `with_regex(pattern, flags=None, threshold=0.0)` - Match regex
- `is_numeric(threshold=0.0)` - Contains only digits
- `is_alpha(threshold=0.0)` - Contains only letters
- `is_alphanumeric(threshold=0.0)` - Letters and digits only
- `is_lowercase(threshold=0.0)` - All lowercase
- `is_uppercase(threshold=0.0)` - All uppercase
- `is_email(threshold=0.0)` - Valid email format
- `is_url(threshold=0.0)` - Valid URL format
- `is_uuid(threshold=0.0)` - Valid UUID format
- `is_unique(threshold=0.0)` - All values unique
- `is_not_null(threshold=0.0)` - No null values

### Integer Columns

```python
from dataguard import integer_column

col = (integer_column("age")
    .between(0, 120)
    .is_not_null())
```

Available integer rules:
- `between(min, max, threshold=0.0)` - Value range
- `min(min, threshold=0.0)` - Minimum value
- `max(max, threshold=0.0)` - Maximum value
- `is_positive(threshold=0.0)` - Greater than 0
- `is_negative(threshold=0.0)` - Less than 0
- `is_non_negative(threshold=0.0)` - Greater than or equal to 0
- `is_non_positive(threshold=0.0)` - Less than or equal to 0
- `is_monotonically_increasing(threshold=0.0)` - Values increase
- `is_monotonically_decreasing(threshold=0.0)` - Values decrease
- `std_dev_check(max_std_dev, threshold=0.0)` - Within N standard deviations
- `mean_variance(max_variance_percent, threshold=0.0)` - Deviation from mean
- `is_unique(threshold=0.0)` - All values unique
- `is_not_null(threshold=0.0)` - No null values

### Float Columns

```python
from dataguard import float_column

col = (float_column("price")
    .is_positive()
    .max(999.99))
```

Float columns support the same rules as integer columns.

### Date Columns

```python
from dataguard import date_column

col = (date_column("created_at", "%Y-%m-%d")
    .is_after(2020, 1, 1)
    .is_not_futur()
    .is_weekday())
```

Available date rules:
- `is_before(year, month=None, day=None, threshold=0.0)` - Before date
- `is_after(year, month=None, day=None, threshold=0.0)` - After date
- `is_not_futur(threshold=0.0)` - Not in the future
- `is_not_past(threshold=0.0)` - Not in the past
- `is_weekday(threshold=0.0)` - Monday through Friday
- `is_weekend(threshold=0.0)` - Saturday or Sunday
- `is_unique(threshold=0.0)` - All values unique
- `is_not_null(threshold=0.0)` - No null values

## Cross-Column Validation

Validate relationships between columns:

```python
from dataguard import CsvTable, date_column, relation

start = date_column("start_date", "%Y-%m-%d")
end = date_column("end_date", "%Y-%m-%d")

# Ensure start_date < end_date
date_relation = relation("start_date", "end_date").date_comparaison("<")

table = CsvTable("events.csv", "events")
table.prepare([start, end], [date_relation])
```

Comparison operators: `"<"`, `"<="`, `"="`, `">="`, `">"`

## Thresholds

All rules accept an optional `threshold` parameter (0.0 to 1.0) that specifies the maximum percentage of rows that can violate the rule:

```python
# Allow up to 5% of emails to be invalid
email = string_column("email").is_email(threshold=0.05)

# Require all ages to be positive
age = integer_column("age").is_positive(threshold=0.0)  # Default
```

## Parquet Support

```python
from dataguard import ParquetTable, integer_column

col = integer_column("user_id").is_positive()

table = ParquetTable("data.parquet", "users")
table.prepare([col])
result = table.validate()
```
