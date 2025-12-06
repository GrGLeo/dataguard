import dataguard
import pandas as pd


def test_unicity(tmp_path):
    data = {"unique_col": ["1", "2", "3", "1", "2", None, None]}
    # Expected errors: 1 (duplicate), 2 (duplicate), None, None
    # None values are ignored for unicity check
    expected_errors = 4

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    guard = dataguard.Guard()
    # Using integer_column, but unicity applies to any column type
    col = dataguard.string_column("unique_col").is_unique()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_unicity_all_unique_with_none(tmp_path):
    data = {"unique_col": ["1", "2", "3", None]}
    # Expected errors: 1 None
    expected_errors = 1

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    guard = dataguard.Guard()
    col = dataguard.string_column("unique_col").is_unique()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_unicity_all_unique(tmp_path):
    data = {"unique_col": ["1", "2", "3", "4"]}
    expected_errors = 0

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    guard = dataguard.Guard()
    col = dataguard.string_column("unique_col").is_unique()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_unicity_all_duplicates(tmp_path):
    data = {"unique_col": ["1", "1", "1", None, None]}
    # Expected errors: 1 (duplicate), 1 (duplicate), None, None
    expected_errors = 4

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    guard = dataguard.Guard()
    col = dataguard.string_column("unique_col").is_unique()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors
