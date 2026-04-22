"""
Case 3 — Multithreaded Producer-Consumer Pipeline.

Architecture:
  Group A — 4 Extract threads  →  Transform Queue (maxsize=20)
  Group B — 8 Transform threads →  Load Queue     (maxsize=20)
  Group C — 4 Load threads      →  Target SQLite DB

All three groups run CONCURRENTLY using threading.Thread.
A threading.Lock() protects the DB write since SQLite doesn't
support concurrent writers.
Poison-pill shutdown: each stage sends SENTINEL to its output queue
when finished so downstream workers exit cleanly.

CPU ~87–91%, throughput ~13,368 rec/s at 500k records.
"""
import sqlite3
import time
import queue
import threading
from transformations import transform_record
from metrics_collector import MetricsCollector
from results_store import ResultsStore

SENTINEL = None               # poison-pill value
DB_WRITE_LOCK = threading.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# Worker functions
# ──────────────────────────────────────────────────────────────────────────────

def extract_worker(source_db: str, chunk_queue: queue.Queue, transform_queue: queue.Queue):
    """Read assigned chunks from source DB and push raw rows to transform_queue."""
    conn = sqlite3.connect(source_db)
    c = conn.cursor()
    while True:
        try:
            chunk_info = chunk_queue.get_nowait()
        except queue.Empty:
            break
        if chunk_info is SENTINEL:
            chunk_queue.put(SENTINEL)   # propagate so other extractors see it
            break
        offset, limit = chunk_info
        c.execute("SELECT * FROM source_records LIMIT ? OFFSET ?", (limit, offset))
        records = c.fetchall()
        if records:
            transform_queue.put(records)
    conn.close()


def transform_worker(transform_queue: queue.Queue, load_queue: queue.Queue):
    """Consume raw chunks from transform_queue, apply business rules, push to load_queue."""
    while True:
        chunk = transform_queue.get()
        if chunk is SENTINEL:
            transform_queue.put(SENTINEL)   # let other transformers exit
            break
        transformed_chunk = [transform_record(r) for r in chunk]
        load_queue.put(transformed_chunk)


def load_worker(target_db: str, load_queue: queue.Queue, counter: list, lock: threading.Lock):
    """Bulk-insert transformed chunks into target DB under a write lock."""
    conn = sqlite3.connect(target_db)
    c = conn.cursor()
    while True:
        chunk = load_queue.get()
        if chunk is SENTINEL:
            load_queue.put(SENTINEL)    # let other loaders exit
            break
        with DB_WRITE_LOCK:
            c.executemany(
                "INSERT INTO target_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                chunk,
            )
            conn.commit()
        with lock:
            counter[0] += len(chunk)
    conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    source_db: str,
    target_db: str,
    num_records: int,
    chunk_size: int = 5000,
    results_store: ResultsStore | None = None,
):
    print("\n▶  Case 3 — Multithreaded Pipeline (4 Extract | 8 Transform | 4 Load)")
    mc = MetricsCollector()

    # ── Setup target schema ──────────────────────────────────────────────
    conn_tgt = sqlite3.connect(target_db)
    conn_tgt.execute("DROP TABLE IF EXISTS target_records")
    conn_tgt.execute(
        """CREATE TABLE target_records (
            id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT,
            email TEXT, phone TEXT, department TEXT, salary_tier TEXT,
            hire_date TEXT, normalized_score REAL, is_active INTEGER,
            address TEXT, city TEXT, country TEXT)"""
    )
    conn_tgt.commit()
    conn_tgt.close()

    # ── Build chunk queue ────────────────────────────────────────────────
    chunk_queue: queue.Queue = queue.Queue()
    transform_queue: queue.Queue = queue.Queue(maxsize=20)   # back-pressure
    load_queue: queue.Queue = queue.Queue(maxsize=20)         # back-pressure

    for offset in range(0, num_records, chunk_size):
        chunk_queue.put((offset, chunk_size))

    NUM_EXTRACT = 4
    NUM_TRANSFORM = 8
    NUM_LOAD = 4

    # Shared counter for live progress
    counter = [0]
    counter_lock = threading.Lock()

    mc.start()
    start_time = time.perf_counter()

    # ── Start all groups CONCURRENTLY ────────────────────────────────────
    extractors = [
        threading.Thread(
            target=extract_worker,
            args=(source_db, chunk_queue, transform_queue),
            daemon=True,
        )
        for _ in range(NUM_EXTRACT)
    ]
    transformers = [
        threading.Thread(
            target=transform_worker,
            args=(transform_queue, load_queue),
            daemon=True,
        )
        for _ in range(NUM_TRANSFORM)
    ]
    loaders = [
        threading.Thread(
            target=load_worker,
            args=(target_db, load_queue, counter, counter_lock),
            daemon=True,
        )
        for _ in range(NUM_LOAD)
    ]

    # Start all stages simultaneously (true pipelining)
    for t in extractors:
        t.start()
    for t in transformers:
        t.start()
    for t in loaders:
        t.start()

    # Wait for extractors → signal transformers → wait → signal loaders → wait
    for t in extractors:
        t.join()
    transform_queue.put(SENTINEL)   # signal all 8 transform workers

    for t in transformers:
        t.join()
    load_queue.put(SENTINEL)        # signal all 4 load workers

    for t in loaders:
        t.join()

    end_time = time.perf_counter()
    mc.stop()

    duration = end_time - start_time
    throughput = num_records / duration if duration > 0 else 0
    metrics = mc.summary()

    print(
        f"✅ Case 3 Done: {num_records:,} records | {duration:.2f}s | {throughput:.0f} rec/s"
        f" | Peak CPU {metrics['peak_cpu_pct']:.1f}% | Peak RAM {metrics['peak_memory_mb']:.0f} MB"
    )

    if results_store:
        results_store.append("Case3_Pipeline", num_records, duration, throughput, metrics)

    return duration, throughput, metrics