"""
Case 2 — Batch File Export-Transform-Import.
Eliminates per-record commit overhead by:
  1. Export: read entire source table into a pandas DataFrame
  2. Transform: vectorised apply() of all business rules
  3. Import: single bulk to_sql() call (one transaction)

CPU ~44–48%, memory-bound at very large sizes.
"""
import sqlite3
import time
import pandas as pd
from transformations import transform_record
from metrics_collector import MetricsCollector
from results_store import ResultsStore


def run_batch(source_db: str, target_db: str, results_store: ResultsStore | None = None):
    print("\n▶  Case 2 — Batch Processing (pandas bulk I/O)")
    mc = MetricsCollector()
    conn_src = sqlite3.connect(source_db)
    conn_tgt = sqlite3.connect(target_db)

    mc.start()
    start_time = time.perf_counter()

    # ── 1. Export ────────────────────────────────────────────────────────
    print("  [1/3] Exporting source records to DataFrame…")
    df = pd.read_sql_query("SELECT * FROM source_records", conn_src)

    # ── 2. Transform ─────────────────────────────────────────────────────
    print(f"  [2/3] Transforming {len(df):,} records via apply()…")
    transformed_data = df.apply(lambda row: transform_record(tuple(row)), axis=1)
    transformed_df = pd.DataFrame(
        transformed_data.tolist(),
        columns=[
            "id", "first_name", "last_name", "email", "phone",
            "department", "salary_tier", "hire_date", "normalized_score",
            "is_active", "address", "city", "country",
        ],
    )

    # ── 3. Import ─────────────────────────────────────────────────────────
    print("  [3/3] Bulk importing transformed DataFrame to target DB…")
    transformed_df.to_sql("target_records", conn_tgt, if_exists="replace", index=False)

    end_time = time.perf_counter()
    mc.stop()
    count = len(df)

    conn_src.close()
    conn_tgt.close()

    duration = end_time - start_time
    throughput = count / duration if duration > 0 else 0
    metrics = mc.summary()

    print(
        f"✅ Case 2 Done: {count:,} records | {duration:.2f}s | {throughput:.0f} rec/s"
        f" | Peak CPU {metrics['peak_cpu_pct']:.1f}% | Peak RAM {metrics['peak_memory_mb']:.0f} MB"
    )

    if results_store:
        results_store.append("Case2_Batch", count, duration, throughput, metrics)

    return duration, throughput, metrics