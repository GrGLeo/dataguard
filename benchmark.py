import time
import pandas as pd
from dataguard import Validator

CSV_PATH = "data/leads-1000000.csv"
NUM_RUNS = 10


def benchmark_pandas():
    times = []
    for _ in range(NUM_RUNS):
        start = time.time()
        df = pd.read_csv(CSV_PATH)
        # Count rows where 'Index' cannot be converted to int
        invalid_count = pd.to_numeric(df["Index"], errors="coerce").isna().sum()
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), invalid_count


def benchmark_validator():
    validator = Validator()
    # Add type check rule for 'Index' as int
    builder = validator.add_column_rule("Index")
    builder.type_check("int")
    times = []
    for _ in range(NUM_RUNS):
        start = time.time()
        error_count = validator.validate_csv(CSV_PATH)
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), error_count


if __name__ == "__main__":
    print("Benchmarking CSV type validation errors for 'Index' column...")
    print(f"Running {NUM_RUNS} times each")

    pandas_avg, pandas_errors = benchmark_pandas()
    validator_avg, validator_errors = benchmark_validator()

    print(f"Pandas average time: {pandas_avg:.6f}s, errors: {pandas_errors}")
    print(f"Validator average time: {validator_avg:.6f}s, errors: {validator_errors}")
    print(
        f"Validator is {pandas_avg / validator_avg:.2f}x faster"
        if validator_avg < pandas_avg
        else f"Pandas is {validator_avg / pandas_avg:.2f}x faster"
    )
