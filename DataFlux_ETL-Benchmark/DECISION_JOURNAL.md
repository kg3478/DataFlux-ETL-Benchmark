# Decision Journal: DataFlux ETL Pipeline
**Role:** AI Product Manager / Technical Lead
**Purpose:** Document critical architectural, product, and engineering decisions made during the development of the DataFlux ETL Benchmark.

---

## Decision 1: Choosing a Multithreaded Pipeline over Batch Processing for Production

**Date:** 2026-04-10
**Context:** When benchmarking ETL architectures to replace our slow sequential baseline, the Pandas-based "Batch" approach was exceptionally fast (7 seconds for 500k records). However, we had to decide whether to standardize on Batch or invest engineering effort into a Multithreaded Pipeline.
**Options Considered:**
1. **Pandas Batch:** Export all data to RAM, vectorize transformations, bulk insert.
2. **Multithreaded Pipeline:** Chunk-based producer-consumer queues.
**Decision:** Selected **Multithreaded Pipeline** as the production standard.
**Rationale (AI PM Perspective):**
* **Scalability for ML Datasets:** Batch processing requires loading the entire dataset into RAM. While fine for 500k records (836 MB), our AI models require training datasets in the hundreds of millions of rows (100GB+). Batch would cause Out-Of-Memory (OOM) crashes in production.
* **Predictable Infrastructure Costs:** The pipeline processes data in chunks with bounded queues, capping memory usage at ~282 MB regardless of whether we process 500k or 50 million records. This allows us to use cheaper compute instances.

---

## Decision 2: Implementing Bounded Queues for Back-Pressure

**Date:** 2026-04-12
**Context:** During initial testing of the Multithreaded Pipeline, we noticed that the Extract threads (reading from source) were much faster than the Transform and Load threads.
**Problem:** Unbounded queues caused the Extract threads to pull the entire database into memory while waiting for Transforms to catch up, defeating the memory-efficiency goal.
**Decision:** Implemented bounded queues (`queue.Queue(maxsize=20)`).
**Rationale (AI PM Perspective):**
* **System Stability:** By capping the queue size, we introduced back-pressure. If the Transform queue is full, the Extract threads automatically block (pause) until space frees up. This guarantees that our memory footprint remains flat and predictable, ensuring high reliability for our automated ML ingestion pipelines.

---

## Decision 3: Threading vs. Multiprocessing in Python

**Date:** 2026-04-14
**Context:** Python's Global Interpreter Lock (GIL) famously prevents true parallel execution of CPU-bound tasks in threads. We needed to choose the concurrency model.
**Options Considered:**
1. `multiprocessing`: True parallel execution, bypasses GIL, but high memory overhead (spawns new OS processes).
2. `threading`: Lightweight, shared memory, but subject to GIL.
**Decision:** Selected `threading`.
**Rationale (AI PM Perspective):**
* **I/O Bound Nature:** Database migrations and ETL are overwhelmingly I/O-bound (waiting on disk reads, network transit, and database fsync/commits), not CPU-bound.
* **GIL Release:** Python releases the GIL during I/O operations. Therefore, threads *can* run concurrently while waiting for the database.
* **Resource Efficiency:** Threads share memory, making it much easier and cheaper to pass chunks of data between Extract, Transform, and Load stages via queues without expensive IPC (Inter-Process Communication) serialization overhead.

---

## Decision 4: Serialized Database Writes (Write Lock)

**Date:** 2026-04-15
**Context:** The Pipeline architecture uses multiple threads for extraction and transformation. We needed to decide how to handle the final Load step into the target database (SQLite).
**Problem:** SQLite (and many relational databases under high load) does not handle highly concurrent write transactions well without locking or throwing "database is locked" errors.
**Decision:** Implemented a central `threading.Lock()` for the Load threads. Only one thread can write to the target database at a time.
**Rationale (AI PM Perspective):**
* **Data Integrity over Absolute Speed:** In AI data pipelines, silent data corruption or lost records completely invalidate model training. Ensuring atomic, locked writes guarantees 100% data integrity.
* **Amdahl’s Law Acceptance:** We accepted this serialization bottleneck because the speedup gained from concurrent extraction and transformation still yielded a 125x overall performance improvement over the baseline.

---

## Decision 5: Standardizing on 500,000 Records for the Benchmark

**Date:** 2026-04-18
**Context:** Determining the upper bound for our benchmark visualizations.
**Decision:** Capped the primary benchmark at 500,000 records.
**Rationale (AI PM Perspective):**
* At 500k records, the sequential baseline takes ~36 minutes. Benchmarking 1 million+ records would take hours just to prove a point we already established. 500k is the perfect "Goldilocks" size—it is large enough to clearly demonstrate the catastrophic failure of sequential processing (36 mins) and the massive speedup of the pipeline (17 seconds), while keeping the entire benchmark suite executable within a reasonable timeframe for peer review.
