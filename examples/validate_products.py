#!/usr/bin/env python3
"""
DataGuard Python Example - Product Validation

This example demonstrates how to use DataGuard to validate a CSV file
containing product data with various validation rules.

These rules match the configuration in examples/config.toml
"""

import dataguard


def main():
    print("DataGuard Example - Product Validation\n")

    # Define validation rules for each column
    # These match the rules in examples/config.toml

    # Index: integer with uniqueness, not null, and positive checks
    index_column = (
        dataguard.integer_column("Index").is_unique().is_not_null().is_positive()
    )

    # Name: string with min/max length and not null
    name_column = (
        dataguard.string_column("Name")
        .is_not_null()
        .with_min_length(3)
        .with_max_length(100)
    )

    # Description: string with min length
    description_column = dataguard.string_column("Description").with_min_length(1)

    # Currency: string with allowed values
    currency_column = dataguard.string_column("Currency").is_in(
        ["USD", "EUR", "GBP", "JPY", "CAD"]
    )

    # Price: integer with range and positive checks
    price_column = dataguard.integer_column("Price").between(1, 10000).is_positive()

    # Stock: integer with non-negative check
    stock_column = dataguard.integer_column("Stock").is_non_negative()

    # EAN: string with exact length and numeric validation
    ean_column = dataguard.string_column("EAN").is_exact_length(13).is_numeric()

    # Availability: string with enumerated values
    availability_column = dataguard.string_column("Availability").is_in(
        [
            "in_stock",
            "out_of_stock",
            "limited_stock",
            "backorder",
            "pre_order",
            "discontinued",
        ]
    )

    # Internal ID: integer with positive check
    internal_id_column = dataguard.integer_column("Internal ID").is_positive()

    # Create and configure table
    table = dataguard.CsvTable(
        "examples/data/products_large.csv", "Products Large Dataset"
    )
    table.prepare(
        [
            index_column,
            name_column,
            description_column,
            currency_column,
            price_column,
            stock_column,
            ean_column,
            availability_column,
            internal_id_column,
        ]
    )

    # Validate the data
    print("Validating products_large.csv (512k rows)...\n")
    result = table.validate()

    # Display results
    print("Validation Results:")
    print(f"  Table: {result['table_name']}")
    print(f"  Total rows: {result['total_rows']}")

    passed, total = result["passed"]
    print(f"  Rules: {passed}/{total} passed")

    if passed == total:
        print("\n✓ All validation rules passed!")
    else:
        print("\n✗ Some validation rules failed.")
        print("  Check the detailed report above for specific errors.")


if __name__ == "__main__":
    main()
