"""
DataGuard: High-performance data validation for CSV and Parquet files.

This package provides a fluent API for defining and executing validation rules
on tabular data with excellent performance.

Example usage:
    >>> from dataguard import CsvTable, string_column, integer_column, date_column
    >>>
    >>> # Define column rules
    >>> name = string_column("name").with_min_length(2).is_not_null()
    >>> age = integer_column("age").between(0, 120)
    >>> birth_date = date_column("birth_date", "%Y-%m-%d").is_not_futur()
    >>>
    >>> # Create and prepare table
    >>> table = CsvTable("users.csv", "users_table")
    >>> table.prepare([name, age, birth_date])
    >>>
    >>> # Validate
    >>> result = table.validate()
    >>> print(f"Validated {result['total_rows']} rows")
    >>> print(f"Passed {result['passed'][0]}/{result['passed'][1]} rules")
"""

from .dataguard import (
    # Table types
    CsvTable,
    ParquetTable,
    # Column builder functions
    string_column,
    integer_column,
    float_column,
    date_column,
    # Relation builder function
    relation,
    # Builder classes (for type hints)
    StringColumnBuilder,
    IntegerColumnBuilder,
    FloatColumnBuilder,
    DateColumnBuilder,
    RelationBuilder,
)

__all__ = [
    # Tables
    "CsvTable",
    "ParquetTable",
    # Column builder functions
    "string_column",
    "integer_column",
    "float_column",
    "date_column",
    # Relation builder
    "relation",
    # Builder classes
    "StringColumnBuilder",
    "IntegerColumnBuilder",
    "FloatColumnBuilder",
    "DateColumnBuilder",
    "RelationBuilder",
]

__version__ = "0.1.0"
