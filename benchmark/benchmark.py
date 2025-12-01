import time
import pandas as pd
import argparse
import sys

sys.path.append("benchmark")
from dataguard import Guard, string_column, integer_column


def benchmark_pandas(csv_path, num_runs):
    times = []
    for i in range(num_runs):
        start = time.time()
        df = pd.read_csv(csv_path)
        end_read = time.time() - start
        print(f"Read: {end_read:.6f}")
        start_valid = time.time()
        # Check type of Category as string (all CSV columns are strings)
        assert df["Category"].dtype == "object", "Category should be string type"
        # Count rows where Category is not 'Home & Kitchen'
        category_invalid = (df["Category"] != "Home & Kitchen").sum()
        # Count rows where Currency length is at least 3
        currency_invalid = (df["Currency"].str.len() >= 3).sum()
        # Count rows where Price is at least 30
        price_invalid = (df["Price"] >= 30).sum()
        # Count rows where Index is not monotonically increasing
        index_invalid = (df["Index"].diff() < 0).iloc[1:].sum()

        # # Check for unicity
        # index_unique_invalid = df["Index"].duplicated().sum()

        end_valid = time.time() - start_valid
        print(f"Validation: {end_valid:.6f}")
        invalid_count = (
            category_invalid + currency_invalid + price_invalid + index_invalid
        )
        # invalid_count = index_unique_invalid
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), invalid_count


def benchmark_validator(csv_path, num_runs):
    # Define column rules using the new API
    category_col = string_column("Category").with_regex(r"^Home & Kitchen$", None)

    currency_col = string_column("Currency").with_min_length(
        min=3
    )  # min_length of 3 means that string.len() < 3 is invalid

    price_col = (
        integer_column("Price")
        .min(min=30)  # Price lesser than 30 are invalid
    )

    index_col = integer_column("Index").is_monotonically_increasing()

    # index_col = (
    #     string_column("Index")
    #     .is_unique()  # Column is unique
    # )

    guard = Guard()
    guard.add_columns([category_col, currency_col, price_col, index_col])
    guard.commit()

    times = []
    for i in range(num_runs):
        start = time.time()
        error_count = guard.validate_csv(csv_path, print_report=False)
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
