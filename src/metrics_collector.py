"""
MetricsCollector — Background thread sampling CPU% and RSS memory every 100ms.
Provides peak, average, and time-series data for the ETL benchmark dashboard.
"""
import threading
import time
import psutil
import os


class MetricsCollector:
    """
    Spawns a daemon thread that samples CPU% and RSS memory (MB) every
    `interval_ms` milliseconds while the ETL run is in progress.
    Call .start() before the ETL and .stop() after to collect results.
    """

    def __init__(self, interval_ms: int = 100):
        self.interval = interval_ms / 1000.0
        self._process = psutil.Process(os.getpid())
        self._samples_cpu: list[float] = []
        self._samples_mem: list[float] = []
        self._timestamps: list[float] = []
        self._running = False
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self):
        """Begin sampling in background daemon thread."""
        self._samples_cpu.clear()
        self._samples_mem.clear()
        self._timestamps.clear()
        self._running = True
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop sampling and wait for thread to finish."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    # ------------------------------------------------------------------
    # Aggregated statistics
    # ------------------------------------------------------------------

    @property
    def peak_cpu(self) -> float:
        return max(self._samples_cpu, default=0.0)

    @property
    def avg_cpu(self) -> float:
        return sum(self._samples_cpu) / len(self._samples_cpu) if self._samples_cpu else 0.0

    @property
    def peak_memory_mb(self) -> float:
        return max(self._samples_mem, default=0.0)

    @property
    def avg_memory_mb(self) -> float:
        return sum(self._samples_mem) / len(self._samples_mem) if self._samples_mem else 0.0

    @property
    def sample_count(self) -> int:
        return len(self._samples_cpu)

    def time_series(self):
        """Return (timestamps, cpu_samples, mem_samples) for plotting."""
        return list(self._timestamps), list(self._samples_cpu), list(self._samples_mem)

    def summary(self) -> dict:
        return {
            "peak_cpu_pct": round(self.peak_cpu, 2),
            "avg_cpu_pct": round(self.avg_cpu, 2),
            "peak_memory_mb": round(self.peak_memory_mb, 2),
            "avg_memory_mb": round(self.avg_memory_mb, 2),
            "sample_count": self.sample_count,
        }

    # ------------------------------------------------------------------
    # Internal sampling loop
    # ------------------------------------------------------------------

    def _sample_loop(self):
        start = time.perf_counter()
        # Warm-up call so first sample is not 0
        try:
            self._process.cpu_percent()
        except psutil.NoSuchProcess:
            return

        while self._running:
            try:
                cpu = self._process.cpu_percent()
                mem = self._process.memory_info().rss / (1024 * 1024)  # bytes → MB
                self._samples_cpu.append(cpu)
                self._samples_mem.append(mem)
                self._timestamps.append(time.perf_counter() - start)
            except psutil.NoSuchProcess:
                break
            time.sleep(self.interval)
