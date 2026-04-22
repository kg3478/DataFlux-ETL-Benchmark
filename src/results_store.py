"""
ResultsStore — Appends benchmark run results to a CSV file so
comparison data persists across multiple runs.
"""
import csv
import os
from datetime import datetime

COLUMNS = [
    "timestamp",
    "case",
    "num_records",
    "duration_s",
    "throughput_rec_s",
    "peak_cpu_pct",
    "avg_cpu_pct",
    "peak_memory_mb",
    "avg_memory_mb",
]


class ResultsStore:
    def __init__(self, csv_path: str = "benchmark_results.csv"):
        self.csv_path = csv_path
        # Write header only if file is new
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writeheader()

    def append(
        self,
        case: str,
        num_records: int,
        duration_s: float,
        throughput_rec_s: float,
        metrics: dict,
    ):
        row = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "case": case,
            "num_records": num_records,
            "duration_s": round(duration_s, 4),
            "throughput_rec_s": round(throughput_rec_s, 2),
            "peak_cpu_pct": metrics.get("peak_cpu_pct", 0),
            "avg_cpu_pct": metrics.get("avg_cpu_pct", 0),
            "peak_memory_mb": metrics.get("peak_memory_mb", 0),
            "avg_memory_mb": metrics.get("avg_memory_mb", 0),
        }
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writerow(row)
        return row

    def load_all(self) -> list[dict]:
        if not os.path.exists(self.csv_path):
            return []
        with open(self.csv_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                row["num_records"] = int(row["num_records"])
                row["duration_s"] = float(row["duration_s"])
                row["throughput_rec_s"] = float(row["throughput_rec_s"])
                row["peak_cpu_pct"] = float(row["peak_cpu_pct"])
                row["avg_cpu_pct"] = float(row["avg_cpu_pct"])
                row["peak_memory_mb"] = float(row["peak_memory_mb"])
                row["avg_memory_mb"] = float(row["avg_memory_mb"])
                rows.append(row)
        return rows
