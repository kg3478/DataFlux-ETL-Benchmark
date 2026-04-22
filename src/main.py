"""
DataFlux ETL Benchmark — main.py (legacy single-run CLI)

For the full multi-size benchmark suite, use:
    python benchmark_runner.py

This script runs a quick single-size benchmark and is retained for
compatibility / quick testing.

Usage:
    python src/main.py --records 10000
    python src/main.py --records 50000 --case 3
"""
import os
import sys
import sqlite3
import argparse

# When run from /src or from project root
sys.path.insert(0, os.path.dirname(__file__))

from data_generator import generate_data
from case1_sequential import run_sequential
from case2_batch import run_batch
from case3_pipeline import run_pipeline
from results_store import ResultsStore


def get_record_count(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM source_records").fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataFlux ETL Benchmark — single-size run")
    parser.add_argument("--records", type=int, default=10000,
                        help="Number of records to generate / process (default 10000)")
    parser.add_argument("--case", type=int, choices=[1, 2, 3],
                        help="Specific case to run (1=Sequential, 2=Batch, 3=Pipeline)")
    args = parser.parse_args()

    source_db = os.path.join(os.path.dirname(__file__), "..", "data", "source.db")
    target_db = os.path.join(os.path.dirname(__file__), "..", "data", "target.db")
    os.makedirs(os.path.dirname(source_db), exist_ok=True)

    csv_path  = os.path.join(os.path.dirname(__file__), "..", "results", "benchmark_results.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    store = ResultsStore(csv_path)

    if get_record_count(source_db) != args.records:
        generate_data(args.records, source_db)

    num_records = get_record_count(source_db)

    if args.case == 1 or not args.case:
        run_sequential(source_db, target_db, results_store=store)
    if args.case == 2 or not args.case:
        run_batch(source_db, target_db, results_store=store)
    if args.case == 3 or not args.case:
        run_pipeline(source_db, target_db, num_records, results_store=store)

    print(f"\nResults saved → {csv_path}")