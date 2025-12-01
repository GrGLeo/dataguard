import dataguard
import pandas as pd
import pytest
from pathlib import Path


def test_between_integer(tmp_path):
    data = {"col": [1, 2, 3, 5, 6, None]}
    # Violations: 1 (too low), 6 (too high), None (null).
    expected_errors = 3
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").between(2, 5)
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_min_integer(tmp_path):
    data = {"col": [1, 2, 3, 5, None]}
    # Violations: 1 (too low), 2 (too low), None (null).
    expected_errors = 3
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").min(3)
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_max_integer(tmp_path):
    data = {"col": [1, 5, 6, 7, None]}
    # Violations: 6 (too high), 7 (too high), None (null).
    expected_errors = 3
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").max(5)
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_is_positive_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: -2, -1, 0, None.
    expected_errors = 4
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").is_positive()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_is_negative_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: 0, 1, 2, None.
    expected_errors = 4
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").is_negative()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_is_non_positive_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: 1, 2, None.
    expected_errors = 3
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").is_non_positive()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_is_non_negative_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: -2, -1, None.
    expected_errors = 3
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").is_non_negative()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_is_monotonically_increasing_integer(tmp_path):
    data = {"col": [1, 2, 2, 4, 3, None, 5]}
    # Violations: 3 (because 3 < 4). None is ignored by monotonicity check.
    # The TypeCheck will count the None as one error. The monotonicity check does not count it.
    # However, the total error count includes the type check error.
    # After the fix, type check should have 0 errors. Let's see. The None will still be a null.
    # The initial StringArray will have a null. The casted Int64Array will have a null. TypeCheck errors = 1 - 1 = 0. Correct.
    # Monotonicity will have 1 error. Total = 1.
    expected_errors = 1
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").is_monotonically_increasing()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors


def test_is_monotonically_decreasing_integer(tmp_path):
    data = {"col": [5, 4, 4, 2, 3, None, 1]}
    # Violations: 3 (because 3 > 2). None is ignored.
    expected_errors = 1
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    guard = dataguard.Guard()
    col = dataguard.integer_column("col").is_monotonically_decreasing()
    guard.add_column(col)
    guard.commit()

    error_count = guard.validate_csv(str(csv_path), print_report=False)
    assert error_count == expected_errors
