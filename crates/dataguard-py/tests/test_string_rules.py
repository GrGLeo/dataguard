import dataguard
import pandas as pd


def test_is_numeric(tmp_path):
    # Data for the test
    data = {"numeric_col": ["123", "456", "abc", "12a", "", None, "789"]}
    # Expected errors: "abc", "12a", "", None

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("numeric_col").is_numeric()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_alpha(tmp_path):
    data = {"alpha_col": ["abc", "XYZ", "aBc", "123", "a1", "", None, "def"]}
    # Expected errors: "123", "a1", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("alpha_col").is_alpha()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_alphanumeric(tmp_path):
    data = {"alphanumeric_col": ["abc", "XYZ123", "aBc-", "123", "a1", "", None, "def"]}
    # Expected errors: "aBc-", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("alphanumeric_col").is_alphanumeric()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_lowercase(tmp_path):
    data = {
        "lowercase_col": ["abc", "xyz", "aBc", "ab c", "ab-c", "123", "", None, "def"]
    }
    # Expected errors: "aBc", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("lowercase_col").is_lowercase()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_uppercase(tmp_path):
    data = {
        "uppercase_col": ["ABC", "XYZ", "aBc", "AB C", "AB-C", "123", "", None, "DEF"]
    }
    # Expected errors: "aBc", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("uppercase_col").is_uppercase()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_with_length_between(tmp_path):
    data = {"col": ["abc", "abcd", "abcde", "ab", "abcdef", "", None]}
    # Violations: "ab" (too short), "abcdef" (too long), "" (too short), None (null).
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("col").with_length_between(3, 5)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_with_min_length(tmp_path):
    data = {"col": ["abc", "abcd", "ab", "", None]}
    # Violations: "ab", "", None.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("col").with_min_length(3)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_with_max_length(tmp_path):
    data = {"col": ["abcde", "abcdef", "abcd", "", None]}
    # Violations: "abcdef", None.
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("col").with_max_length(5)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_with_regex(tmp_path):
    data = {"col": ["ABC-123", "XYZ-456", "abc-123", "ABC-12", "ABC-1234", "", None]}
    # Violations: "abc-123", "ABC-12", "ABC-1234", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("col").with_regex(r"^[A-Z]{3}-\d{3}$", None)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_url(tmp_path):
    data = {
        "url_col": [
            "http://example.com",
            "https://www.google.com/search?q=rust",
            "ftp://ftp.mozilla.org/pub/",
            "http://127.0.0.1:8080/path",
            "www.example.com",  # missing scheme
            "example.com",  # missing scheme
            "http:// bad .com",  # spaces
            "not-a-url",
            "",  # empty string
            None,
        ]
    }
    # Expected Errors: "www.example.com", "example.com", "http:// bad .com", "not-a-url", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("url_col").is_url()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_email(tmp_path):
    data = {
        "email_col": [
            "test@example.com",
            "first.last@sub.example.co.uk",
            "user+tag@domain.net",
            "123user@example.org",
            "invalid-email",  # no @
            "user@domain",  # no TLD
            "user@domain.",  # TLD too short
            "@domain.com",  # missing local part
            "user@domain..com",  # double dot
            "",  # empty string
            None,
        ]
    }
    # Expected Errors: "invalid-email", "user@domain", "user@domain.", "@domain.com", "user@domain..com", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("email_col").is_email()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_uuid(tmp_path):
    data = {
        "uuid_col": [
            "a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5",
            "00000000-0000-0000-0000-000000000000",
            "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
            "87654321-abcd-efab-1234-567890abcdef",
            "not-a-uuid",  # not a uuid
            "1234-5678-90ab-cdef-1234567890ab",  # wrong format
            "a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5x",  # extra char
            "g1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5",  # invalid hex
            "",  # empty string
            None,
        ]
    }
    # Expected Errors: "not-a-uuid", "1234-5678-90ab-cdef-1234567890ab", "a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5x", "g1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5", "", None
    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("uuid_col").is_uuid()
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total


def test_is_in_check(tmp_path):
    data = {
        "fruit_col": [
            "apple",
            "banana",
            "orange",
            "grape",  # Not in allowed_values
            "Apple",  # Different case, not in allowed_values
            "",  # Empty string, not in allowed_values
            None,  # Null, not in allowed_values
        ]
    }
    # Expected errors: "grape", "Apple", "", None
    allowed_values = ["apple", "banana", "orange"]

    csv_path = tmp_path / "test.csv"
    pd.DataFrame(data).to_csv(csv_path, index=False)

    col = dataguard.string_column("fruit_col").is_in(allowed_values)
    table = dataguard.CsvTable(str(csv_path), "test_table")
    table.prepare([col])

    result = table.validate()
    passed, total = result["passed"]
    assert passed < total
