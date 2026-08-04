"""Random-sample 1000 ts_code from stk_info, query stk_daily per code for
2026-06-01..2026-07-31, and report the total running time."""
import random
import statistics
import time

import duckdb

DB_PATH = "/data/tushare_persistence/duckdb/tushare.duckdb"
SEED = 42
N = 1000
START_DATE = "2026-06-01"
END_DATE = "2026-07-31"


def main():
    random.seed(SEED)

    t0 = time.perf_counter()
    con = duckdb.connect(DB_PATH, read_only=True)

    codes = [r[0] for r in con.execute("SELECT ts_code FROM stk_info").fetchall()]
    sample = random.sample(codes, min(N, len(codes)))
    print(f"[setup] sampled {len(sample)} codes from {len(codes)} in {time.perf_counter() - t0:.2f}s")

    query_times = []
    total_rows = 0
    empty_codes = 0

    run_start = time.perf_counter()
    for i, code in enumerate(sample, 1):
        q0 = time.perf_counter()
        rows = con.execute(
            "SELECT * FROM stk_daily WHERE ts_code = ? AND trade_date BETWEEN ? AND ?",
            [code, START_DATE, END_DATE],
        ).fetchall()
        q1 = time.perf_counter()

        dt = q1 - q0
        query_times.append(dt)
        total_rows += len(rows)
        if not rows:
            empty_codes += 1

        if i % 100 == 0:
            elapsed = time.perf_counter() - run_start
            print(
                f"  [{i}/{len(sample)}] elapsed {elapsed:.1f}s | "
                f"avg {(sum(query_times) / i) * 1000:.1f}ms/query"
            )

    total = time.perf_counter() - run_start
    con.close()

    print("\n===== REPORT =====")
    print(f"sample size            : {len(sample)}")
    print(f"date range             : {START_DATE} .. {END_DATE}")
    print(f"total query time       : {total:.2f}s")
    print(f"avg query time         : {statistics.mean(query_times) * 1000:.2f} ms")
    print(f"median query time      : {statistics.median(query_times) * 1000:.2f} ms")
    print(f"min / max query time   : {min(query_times) * 1000:.2f} / {max(query_times) * 1000:.2f} ms")
    print(f"rows returned (total)  : {total_rows}")
    print(f"codes with no data     : {empty_codes} / {len(sample)}")


if __name__ == "__main__":
    main()
