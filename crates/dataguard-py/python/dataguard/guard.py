# This file is kept for backwards compatibility but is now empty.
# The new API uses CsvTable and ParquetTable directly without a Guard/Validator wrapper.
#
# Example usage:
# ```python
# from dataguard import CsvTable, string_column, integer_column
#
# # Create columns
# name_col = string_column("name").with_min_length(3)
# age_col = integer_column("age").is_positive()
#
# # Create table and prepare
# table = CsvTable("data.csv", "my_table")
# table.prepare([name_col, age_col])
#
# # Validate
# result = table.validate()
# print(f"Validated {result['total_rows']} rows")
# print(f"Passed {result['passed'][0]}/{result['passed'][1]} rules")
# ```
