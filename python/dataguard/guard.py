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
    Base class for all column types.
    """

    def __init__(self, name: str, builder):
        self.name = name
        self._builder = builder

    def is_unique(self) -> "Column":
        self._builder = self._builder.is_unique()
        return self

    def _build(self) -> RustColumn:
        """Calls the rust builder to create a rust Column object"""
        return self._builder.build()


class StringColumn(Column):
    """
    A class used to define rules on a string column.
    """

    def with_regex(self, pattern: str, name: Optional[str] = None) -> "StringColumn":
        self._builder = self._builder.with_regex(pattern, name)
        return self

    def with_min_length(self, min: int) -> "StringColumn":
        self._builder = self._builder.with_min_length(min)
        return self

    def with_max_length(self, max: int) -> "StringColumn":
        self._builder = self._builder.with_max_length(max)
        return self

    def with_length_between(
        self, min: Optional[int], max: Optional[int]
    ) -> "StringColumn":
        self._builder = self._builder.with_length_between(min, max)
        return self

    def is_numeric(self) -> "StringColumn":
        self._builder = self._builder.is_numeric()
        return self

    def is_alpha(self) -> "StringColumn":
        self._builder = self._builder.is_alpha()
        return self

    def is_alphanumeric(self) -> "StringColumn":
        self._builder = self._builder.is_alphanumeric()
        return self

    def is_lowercase(self) -> "StringColumn":
        self._builder = self._builder.is_lowercase()
        return self

    def is_uppercase(self) -> "StringColumn":
        self._builder = self._builder.is_uppercase()
        return self

    def is_url(self) -> "StringColumn":
        self._builder = self._builder.is_url()
        return self

    def is_email(self) -> "StringColumn":
        self._builder = self._builder.is_email()
        return self

    def is_uuid(self) -> "StringColumn":
        self._builder = self._builder.is_uuid()
        return self

    def is_in(self, values: List[str]) -> "StringColumn":
        self._builder = self._builder.is_in(values)
        return self


class NumericColumn(Column):
    """
    A base class for numeric columns (integer and float).
    """

    def min(self, min: Union[int, float]) -> "NumericColumn":
        self._builder = self._builder.min(min)
        return self

    def max(self, max: Union[int, float]) -> "NumericColumn":
        self._builder = self._builder.max(max)
        return self

    def between(
        self, min: Optional[Union[int, float]], max: Optional[Union[int, float]]
    ) -> "NumericColumn":
        self._builder = self._builder.between(min, max)
        return self

    def is_positive(self) -> "NumericColumn":
        self._builder = self._builder.is_positive()
        return self

    def is_negative(self) -> "NumericColumn":
        self._builder = self._builder.is_negative()
        return self

    def is_non_positive(self) -> "NumericColumn":
        self._builder = self._builder.is_non_positive()
        return self

    def is_non_negative(self) -> "NumericColumn":
        self._builder = self._builder.is_non_negative()
        return self

    def is_monotonically_increasing(self) -> "NumericColumn":
        self._builder = self._builder.is_monotonically_increasing()
        return self

    def is_monotonically_decreasing(self) -> "NumericColumn":
        self._builder = self._builder.is_monotonically_decreasing()
        return self


class IntegerColumn(NumericColumn):
    """
    A class used to define rules on an integer column.
    """


class FloatColumn(NumericColumn):
    """
    A class used to define rules on a float column.
    """


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


def string_column(name: str) -> StringColumn:
    return StringColumn(name, rust_string_column(name))


def integer_column(name: str) -> IntegerColumn:
    return IntegerColumn(name, rust_integer_column(name))


def float_column(name: str) -> FloatColumn:
    return FloatColumn(name, rust_float_column(name))
