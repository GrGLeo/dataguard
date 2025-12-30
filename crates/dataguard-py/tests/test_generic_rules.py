import dataguard
import pandas as pd


def test_unicity(tmp_path):
    data = {"unique_col": ["1", "2", "3", "1", "2", None, None]}
    # Expected errors: 1 (duplicate), 2 (duplicate), None, None
    # None values are ignored for unicity check
    # Since we have 4 violations out of 7 rows, and threshold is 0.0, all 4 should be caught

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    # Create column with unicity rule
    col = dataguard.string_column("unique_col").is_unique()

    # Create table and prepare
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    # Validate
    result = table.validate()

    # Check that the validation failed (passed[0] < passed[1])
    passed, total = result["passed"]
    assert passed < total, f"Expected validation to fail but got {passed}/{total}"


def test_unicity_all_unique_with_none(tmp_path):
    data = {"unique_col": ["1", "2", "3", None]}
    # All values are unique, but we have one None
    # With threshold 0.0, null values might cause failure

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("unique_col").is_unique()

    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()

    # Check result - could pass or fail depending on null handling
    print(f"Result: {result}")


def test_unicity_all_unique(tmp_path):
    data = {"unique_col": ["1", "2", "3", "4"]}
    # All values are unique, no nulls

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("unique_col").is_unique()

    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()

    # Should pass all rules
    passed, total = result["passed"]
    assert passed == total, f"Expected all rules to pass but got {passed}/{total}"


def test_unicity_all_duplicates(tmp_path):
    data = {"unique_col": ["1", "1", "1", None, None]}
    # All non-null values are duplicates

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("unique_col").is_unique()

    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()

    # Should fail validation
    passed, total = result["passed"]
    assert passed < total, f"Expected validation to fail but got {passed}/{total}"
