import time
import pandas as pd
from dataguard import Validator

CSV_PATH = "data/leads-100000.csv"
NUM_RUNS = 100


def benchmark_pandas():
    times = []
    for _ in range(NUM_RUNS):
        start = time.time()
        df = pd.read_csv(CSV_PATH)
        count = df.shape[0]
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), count


def benchmark_validator():
    validator = Validator()
    times = []
    for _ in range(NUM_RUNS):
        start = time.time()
        count = validator.validate_csv(CSV_PATH)
        end = time.time()
        times.append(end - start)
    return sum(times) / len(times), count


if __name__ == "__main__":
    print("Benchmarking CSV row counting...")
    print(f"Running {NUM_RUNS} times each")

    pandas_avg, pandas_count = benchmark_pandas()
    validator_avg, validator_count = benchmark_validator()

    print(f"Pandas average time: {pandas_avg:.6f}s, count: {pandas_count}")
    print(f"Validator average time: {validator_avg:.6f}s, count: {validator_count}")
    print(
        f"Validator is {pandas_avg / validator_avg:.2f}x faster"
        if validator_avg < pandas_avg
        else f"Pandas is {validator_avg / pandas_avg:.2f}x faster"
    )
