from typing import List, Optional, Union
from .dataguard import (
    Validator as RustValidator,
    string_column as rust_string_column,
    integer_column as rust_integer_column,
    float_column as rust_float_column,
    Column as RustColumn,
)


class Column:
    """
    A class used to define rules on a column. This is a wrapper around the rust
    column builders.
    """

    def __init__(self, name: str, dtype: str):
        self.name = name
        self.dtype = dtype
        if dtype == "string":
            self._builder = rust_string_column(name)
        elif dtype == "integer":
            self._builder = rust_integer_column(name)
        elif dtype == "float":
            self._builder = rust_float_column(name)
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

    def with_regex(
        self, pattern: str, name: Optional[str] = None
    ) -> "Column":
        self._builder = self._builder.with_regex(pattern, name)
        return self

    def with_min_length(self, min: int) -> "Column":
        self._builder = self._builder.with_min_length(min)
        return self

    def with_max_length(self, max: int) -> "Column":
        self._builder = self._builder.with_max_length(max)
        return self

    def with_length_between(
        self, min: Optional[int], max: Optional[int]
    ) -> "Column":
        self._builder = self._builder.with_length_between(min, max)
        return self

    def min(self, min: Union[int, float]) -> "Column":
        self._builder = self._builder.min(min)
        return self

    def max(self, max: Union[int, float]) -> "Column":
        self._builder = self._builder.max(max)
        return self

    def between(
        self, min: Optional[Union[int, float]], max: Optional[Union[int, float]]
    ) -> "Column":
        self._builder = self._builder.between(min, max)
        return self

    def is_positive(self) -> "Column":
        self._builder = self._builder.is_positive()
        return self

    def is_negative(self) -> "Column":
        self._builder = self._builder.is_negative()
        return self

    def is_non_positive(self) -> "Column":
        self._builder = self._builder.is_non_positive()
        return self

    def is_non_negative(self) -> "Column":
        self._builder = self._builder.is_non_negative()
        return self

    def is_monotonically_increasing(self) -> "Column":
        self._builder = self._builder.is_monotonically_increasing()
        return self

    def is_monotonically_decreasing(self) -> "Column":
        self._builder = self._builder.is_monotonically_decreasing()
        return self

    def is_unique(self) -> "Column":
        self._builder = self._builder.is_unique()
        return self

    def is_numeric(self) -> "Column":
        self._builder = self._builder.is_numeric()
        return self

    def is_alpha(self) -> "Column":
        self._builder = self._builder.is_alpha()
        return self

    def is_alphanumeric(self) -> "Column":
        self._builder = self._builder.is_alphanumeric()
        return self

    def is_lowercase(self) -> "Column":
        self._builder = self._builder.is_lowercase()
        return self

    def is_uppercase(self) -> "Column":
        self._builder = self._builder.is_uppercase()
        return self

    def is_url(self) -> "Column":
        self._builder = self._builder.is_url()
        return self

    def is_email(self) -> "Column":
        self._builder = self._builder.is_email()
        return self

    def is_uuid(self) -> "Column":
        self._builder = self._builder.is_uuid()
        return self

    def is_in(self, values: List[str]) -> "Column":
        self._builder = self._builder.is_in(values)
        return self

    def _build(self) -> RustColumn:
        """Calls the rust builder to create a rust Column object"""
        return self._builder.build()


class Guard:
    """
    Guard is the main entrypoint of the dataguard library. It is used to
    create a validation pipeline and run it on a CSV file.
    """

    def __init__(self):
        self._validator = RustValidator()
        self.columns: List[Column] = []

    def add_column(self, column: Column):
        """Adds a column to the guard"""
        self.columns.append(column)

    def add_columns(self, columns: List[Column]):
        """Adds a list of columns to the guard"""
        self.columns.extend(columns)

    def commit(self):
        """
        Commits the columns to the rust validator. This will build the columns
        and pass them to the rust validator.
        """
        rust_columns = [col._build() for col in self.columns]
        self._validator.commit(rust_columns)

    def validate_csv(self, path: str, print_report: bool = False) -> int:
        return self._validator.validate_csv(path, print_report)


def string_column(name: str) -> Column:
    return Column(name, "string")


def integer_column(name: str) -> Column:
    return Column(name, "integer")


def float_column(name: str) -> Column:
    return Column(name, "float")
