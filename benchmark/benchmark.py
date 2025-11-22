import time
import pandas as pd
import argparse
import sys

sys.path.append("benchmark")
from dataguard import Validator


def benchmark_pandas(csv_path, num_runs):
    times = []
    for i in range(num_runs):
        start = time.time()
        df = pd.read_csv(csv_path)
        # Check type of Category as string (all CSV columns are strings)
        assert df["Category"].dtype == "object", "Category should be string type"
        # Count rows where Category is not 'Home & Kitchen'
        category_invalid = (df["Category"] != "Home & Kitchen").sum()
        # Count rows where Currency length is less than 3
        currency_invalid = (df["Currency"].str.len() < 3).sum()
        invalid_count = category_invalid + currency_invalid
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), invalid_count


def benchmark_validator(csv_path, num_runs):
    validator = Validator()
    # Add type check rule for 'Category' as string and regex match for 'Home & Kitchen'
    builder = validator.add_column_rule("Category")
    builder.type_check("string")
    builder.regex_match(r"^Home & Kitchen$", None)

    builder = validator.add_column_rule("Currency")
    builder.type_check("string")
    builder.string_length_check(3, "lt")

    times = []
    for i in range(num_runs):
        start = time.time()
        error_count = validator.validate_csv(csv_path)
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), error_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CSV validation benchmarks")
    parser.add_argument(
        "-n", "--runs", type=int, default=10, help="Number of benchmark runs"
    )
    parser.add_argument(
        "-s",
        "--size",
        type=str,
        default="small",
        help="Size of csv file to use, small, medium or large",
    )
    args = parser.parse_args()

    csv_path = f"benchmark/products_{args.size}.csv"

    print(f"Running {args.runs} times each")

    pandas_avg, pandas_errors = benchmark_pandas(csv_path, args.runs)
    validator_avg, validator_errors = benchmark_validator(csv_path, args.runs)

    print(f"Pandas average time: {pandas_avg:.6f}s, errors: {pandas_errors}")
    print(f"Validator average time: {validator_avg:.6f}s, errors: {validator_errors}")
    print(
        f"Validator is {pandas_avg / validator_avg:.2f}x faster"
        if validator_avg < pandas_avg
        else f"Pandas is {validator_avg / pandas_avg:.2f}x faster"
    )
