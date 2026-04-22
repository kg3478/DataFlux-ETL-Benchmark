"""
Case 1 — Sequential ETL (record-by-record with per-record commit).
This is the baseline: every INSERT is wrapped in its own COMMIT,
which is the classic I/O bottleneck (CPU ~10%, disk-wait dominated).
"""
import sqlite3
import time
from transformations import transform_record
from metrics_collector import MetricsCollector
from results_store import ResultsStore


def run_sequential(source_db: str, target_db: str, results_store: ResultsStore | None = None):
    print("\n▶  Case 1 — Sequential Processing (per-record COMMIT)")
    mc = MetricsCollector()

    conn_src = sqlite3.connect(source_db)
    conn_tgt = sqlite3.connect(target_db)

    # ── Setup target schema ──────────────────────────────────────────────
    conn_tgt.execute("DROP TABLE IF EXISTS target_records")
    conn_tgt.execute(
        """CREATE TABLE target_records (
            id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT,
            email TEXT, phone TEXT, department TEXT, salary_tier TEXT,
            hire_date TEXT, normalized_score REAL, is_active INTEGER,
            address TEXT, city TEXT, country TEXT)"""
    )
    conn_tgt.commit()

    c_src = conn_src.cursor()
    c_tgt = conn_tgt.cursor()
    c_src.execute("SELECT * FROM source_records")

    mc.start()
    start_time = time.perf_counter()
    count = 0

    for row in c_src:
        transformed = transform_record(row)
        c_tgt.execute(
            "INSERT INTO target_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            transformed,
        )
        conn_tgt.commit()  # ← per-record commit: the intentional bottleneck
        count += 1
        if count % 500 == 0:
            print(f"  Case 1 progress: {count:,} records processed", end="\r")

    end_time = time.perf_counter()
    mc.stop()

    conn_src.close()
    conn_tgt.close()

    duration = end_time - start_time
    throughput = count / duration if duration > 0 else 0
    metrics = mc.summary()

    print(
        f"\n✅ Case 1 Done: {count:,} records | {duration:.2f}s | {throughput:.0f} rec/s"
        f" | Peak CPU {metrics['peak_cpu_pct']:.1f}% | Peak RAM {metrics['peak_memory_mb']:.0f} MB"
    )

    if results_store:
        results_store.append("Case1_Sequential", count, duration, throughput, metrics)

    return duration, throughput, metrics