import dataguard
import pandas as pd


def test_between_integer(tmp_path):
    data = {"col": [1, 2, 3, 5, 6, None]}
    # Violations: 1 (too low), 6 (too high), None (null).
    # With new API, we check if validation failed
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").between(2, 5)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    # Should fail because there are violations
    assert passed < total


def test_min_integer(tmp_path):
    data = {"col": [1, 2, 3, 5, None]}
    # Violations: 1 (too low), 2 (too low), None (null).
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").min(3)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_max_integer(tmp_path):
    data = {"col": [1, 5, 6, 7, None]}
    # Violations: 6 (too high), 7 (too high), None (null).
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").max(5)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_positive_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: -2, -1, 0, None.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").is_positive()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_negative_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: 0, 1, 2, None.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").is_negative()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_non_positive_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: 1, 2, None.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").is_non_positive()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_non_negative_integer(tmp_path):
    data = {"col": [-2, -1, 0, 1, 2, None]}
    # Violations: -2, -1, None.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").is_non_negative()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_monotonically_increasing_integer(tmp_path):
    data = {"col": [1, 2, 2, 4, 3, None, 5]}
    # Violations: 3 (because 3 < 4). None is ignored by monotonicity check.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").is_monotonically_increasing()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_monotonically_decreasing_integer(tmp_path):
    data = {"col": [5, 4, 4, 2, 3, None, 1]}
    # Violations: 3 (because 3 > 2). None is ignored.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame({"col": pd.Series(data["col"], dtype="Int64")}).to_csv(
        csv_path, index=False
    )

    col = dataguard.integer_column("col").is_monotonically_decreasing()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total
