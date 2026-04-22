"""
benchmark_runner.py — Runs the full DataFlux ETL benchmark across 5 record-count
configurations and persists every result to CSV for chart generation.

Record counts per spec: 10_000 / 50_000 / 100_000 / 250_000 / 500_000

Usage:
    python benchmark_runner.py                     # full suite
    python benchmark_runner.py --quick             # 10k + 50k only
    python benchmark_runner.py --records 100000    # single size, all cases
    python benchmark_runner.py --case 3            # pipeline only (all sizes)
"""
import os
import sys
import sqlite3
import argparse
import time

# Add src to path when run from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from data_generator import generate_data
from case1_sequential import run_sequential
from case2_batch import run_batch
from case3_pipeline import run_pipeline
from results_store import ResultsStore

# ─────────────────────────────────────────────────────────────────────────────
RECORD_SIZES = [10_000, 50_000, 100_000, 250_000, 500_000]
SOURCE_DB    = os.path.join(os.path.dirname(__file__), "data", "source.db")
TARGET_DB    = os.path.join(os.path.dirname(__file__), "data", "target.db")
CSV_PATH     = os.path.join(os.path.dirname(__file__), "results", "benchmark_results.csv")
# ─────────────────────────────────────────────────────────────────────────────


def get_record_count(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM source_records").fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


def ensure_dirs():
    os.makedirs(os.path.dirname(SOURCE_DB), exist_ok=True)
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)


def run_benchmark(record_sizes: list[int], cases: list[int]):
    ensure_dirs()
    store = ResultsStore(CSV_PATH)

    print("=" * 70)
    print("  DataFlux ETL Benchmark")
    print(f"  Record sizes : {[f'{n:,}' for n in record_sizes]}")
    print(f"  Cases        : {cases}")
    print("=" * 70)

    total_start = time.perf_counter()

    for n in record_sizes:
        print(f"\n{'─'*70}")
        print(f"  RECORD COUNT: {n:,}")
        print(f"{'─'*70}")

        # Generate / verify source data
        if get_record_count(SOURCE_DB) != n:
            print(f"  Generating {n:,} synthetic records…")
            generate_data(n, SOURCE_DB)
        else:
            print(f"  ✓ Source DB already has {n:,} records — skipping generation.")

        if 1 in cases:
            # Case 1 is slow for large sizes — warn user
            if n > 50_000:
                print(f"\n  ⚠  Case 1 at {n:,} records can take {n/231:.0f}s (~{n/231/60:.1f} min). Press Ctrl+C to skip.\n")
            run_sequential(SOURCE_DB, TARGET_DB, results_store=store)

        if 2 in cases:
            run_batch(SOURCE_DB, TARGET_DB, results_store=store)

        if 3 in cases:
            run_pipeline(SOURCE_DB, TARGET_DB, n, results_store=store)

    total = time.perf_counter() - total_start
    print(f"\n{'='*70}")
    print(f"  Benchmark complete in {total:.1f}s  |  Results → {CSV_PATH}")
    print(f"{'='*70}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DataFlux ETL Benchmark — runs 3 ETL architectures across 5 record sizes"
    )
    parser.add_argument(
        "--records", type=int, default=None,
        help="Run for a single record count only (e.g. --records 50000)"
    )
    parser.add_argument(
        "--case", type=int, choices=[1, 2, 3], default=None,
        help="Run only one case (1=Sequential, 2=Batch, 3=Pipeline)"
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick run: 10k + 50k only (skips Case 1 for larger sizes)"
    )
    parser.add_argument(
        "--skip-case1", action="store_true",
        help="Skip Case 1 (very slow at large record counts)"
    )
    args = parser.parse_args()

    if args.records:
        sizes = [args.records]
    elif args.quick:
        sizes = [10_000, 50_000]
    else:
        sizes = RECORD_SIZES

    if args.case:
        cases = [args.case]
    elif args.skip_case1:
        cases = [2, 3]
    else:
        cases = [1, 2, 3]

    run_benchmark(sizes, cases)
