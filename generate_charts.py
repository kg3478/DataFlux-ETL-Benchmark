"""
generate_charts.py — Creates all 5 benchmark visualisation charts from
the benchmark_results.csv produced by benchmark_runner.py.

Charts produced (Plotly HTML + Matplotlib PNG):
  1. Total Execution Time vs Record Count (all 3 cases)
  2. Throughput (rec/s) vs Record Count  (all 3 cases)
  3. CPU Utilisation Comparison bar chart (peak + avg, all cases)
  4. Memory Usage Comparison bar chart   (peak + avg, all cases)
  5. Speedup Ratio (Case 3 / Case 1) vs Record Count

Usage:
    python generate_charts.py                           # reads default CSV
    python generate_charts.py --csv path/to/file.csv   # custom CSV
"""
import os
import sys
import argparse

import matplotlib
matplotlib.use("Agg")           # non-interactive backend for headless environments
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import plotly.graph_objects as go
from plotly.subplots import make_subplots

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from results_store import ResultsStore

# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CSV    = os.path.join(os.path.dirname(__file__), "results", "benchmark_results.csv")
CHARTS_DIR     = os.path.join(os.path.dirname(__file__), "results", "charts")
PLOTLY_COLORS  = {
    "Case1_Sequential": "#EF4444",   # red
    "Case2_Batch":      "#F59E0B",   # amber
    "Case3_Pipeline":   "#10B981",   # emerald
}
MPL_COLORS = {
    "Case1_Sequential": "#EF4444",
    "Case2_Batch":      "#F59E0B",
    "Case3_Pipeline":   "#10B981",
}
CASE_LABELS = {
    "Case1_Sequential": "Case 1 — Sequential",
    "Case2_Batch":      "Case 2 — Batch",
    "Case3_Pipeline":   "Case 3 — Pipeline",
}
# ─────────────────────────────────────────────────────────────────────────────


def _group_by_case(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for r in rows:
        grouped.setdefault(r["case"], []).append(r)
    # Sort each group by num_records
    for case in grouped:
        grouped[case].sort(key=lambda x: x["num_records"])
    return grouped


def _ensure_output_dir():
    os.makedirs(CHARTS_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Chart 1 — Execution Time vs Record Count
# ─────────────────────────────────────────────────────────────────────────────

def chart_execution_time(grouped: dict, out_dir: str):
    fig_html = go.Figure()
    fig_mpl, ax = plt.subplots(figsize=(10, 6))

    for case, rows in grouped.items():
        x = [r["num_records"] for r in rows]
        y = [r["duration_s"] for r in rows]
        label = CASE_LABELS.get(case, case)
        color = PLOTLY_COLORS.get(case, "#888")

        fig_html.add_trace(go.Scatter(
            x=x, y=y, mode="lines+markers", name=label,
            line=dict(color=color, width=3),
            marker=dict(size=9),
        ))
        ax.plot(x, y, "o-", label=label, color=MPL_COLORS.get(case, "#888"),
                linewidth=2.5, markersize=7)

    # Plotly
    fig_html.update_layout(
        title="📊 Chart 1 — Total Execution Time vs Record Count",
        xaxis_title="Number of Records",
        yaxis_title="Execution Time (seconds)",
        legend=dict(x=0.01, y=0.99),
        template="plotly_dark",
        font=dict(family="Inter, sans-serif", size=13),
    )
    fig_html.write_html(os.path.join(out_dir, "chart1_execution_time.html"))

    # Matplotlib
    ax.set_xlabel("Number of Records", fontsize=13)
    ax.set_ylabel("Execution Time (seconds)", fontsize=13)
    ax.set_title("Total Execution Time vs Record Count", fontsize=15, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    fig_mpl.tight_layout()
    fig_mpl.savefig(os.path.join(out_dir, "chart1_execution_time.png"), dpi=150)
    plt.close(fig_mpl)
    print("  ✓ Chart 1 — Execution Time")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 2 — Throughput vs Record Count
# ─────────────────────────────────────────────────────────────────────────────

def chart_throughput(grouped: dict, out_dir: str):
    fig_html = go.Figure()
    fig_mpl, ax = plt.subplots(figsize=(10, 6))

    for case, rows in grouped.items():
        x = [r["num_records"] for r in rows]
        y = [r["throughput_rec_s"] for r in rows]
        label = CASE_LABELS.get(case, case)
        color = PLOTLY_COLORS.get(case, "#888")

        fig_html.add_trace(go.Scatter(
            x=x, y=y, mode="lines+markers", name=label,
            line=dict(color=color, width=3),
            marker=dict(size=9),
        ))
        ax.plot(x, y, "o-", label=label, color=MPL_COLORS.get(case, "#888"),
                linewidth=2.5, markersize=7)

    fig_html.update_layout(
        title="📊 Chart 2 — Throughput (rec/s) vs Record Count",
        xaxis_title="Number of Records",
        yaxis_title="Throughput (records / second)",
        legend=dict(x=0.01, y=0.99),
        template="plotly_dark",
        font=dict(family="Inter, sans-serif", size=13),
    )
    fig_html.write_html(os.path.join(out_dir, "chart2_throughput.html"))

    ax.set_xlabel("Number of Records", fontsize=13)
    ax.set_ylabel("Throughput (rec/s)", fontsize=13)
    ax.set_title("Throughput vs Record Count", fontsize=15, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{int(y):,}"))
    fig_mpl.tight_layout()
    fig_mpl.savefig(os.path.join(out_dir, "chart2_throughput.png"), dpi=150)
    plt.close(fig_mpl)
    print("  ✓ Chart 2 — Throughput")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 3 — CPU Utilisation Comparison (grouped bar)
# ─────────────────────────────────────────────────────────────────────────────

def chart_cpu(grouped: dict, out_dir: str):
    """Use the largest common record count for the bar chart."""
    # Collect data: for each case use the row with the most records
    cases = list(grouped.keys())
    peak_vals = [max(grouped[c], key=lambda r: r["num_records"])["peak_cpu_pct"] for c in cases]
    avg_vals  = [max(grouped[c], key=lambda r: r["num_records"])["avg_cpu_pct"]  for c in cases]
    labels    = [CASE_LABELS.get(c, c) for c in cases]
    colors    = [PLOTLY_COLORS.get(c, "#888") for c in cases]

    # Plotly grouped bar
    fig_html = go.Figure(data=[
        go.Bar(name="Peak CPU %", x=labels, y=peak_vals,
               marker_color=colors, opacity=0.9),
        go.Bar(name="Avg CPU %",  x=labels, y=avg_vals,
               marker_color=colors, opacity=0.55),
    ])
    fig_html.update_layout(
        barmode="group",
        title="📊 Chart 3 — CPU Utilisation Comparison (at max record count)",
        yaxis_title="CPU Utilisation (%)",
        template="plotly_dark",
        font=dict(family="Inter, sans-serif", size=13),
    )
    fig_html.write_html(os.path.join(out_dir, "chart3_cpu_comparison.html"))

    # Matplotlib
    import numpy as np
    x = np.arange(len(labels))
    width = 0.35
    fig_mpl, ax = plt.subplots(figsize=(9, 6))
    ax.bar(x - width/2, peak_vals, width, label="Peak CPU %", color=colors, alpha=0.9)
    ax.bar(x + width/2, avg_vals,  width, label="Avg CPU %",  color=colors, alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel("CPU Utilisation (%)", fontsize=13)
    ax.set_title("CPU Utilisation Comparison", fontsize=15, fontweight="bold")
    ax.legend(fontsize=11)
    ax.set_ylim(0, 110)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    fig_mpl.tight_layout()
    fig_mpl.savefig(os.path.join(out_dir, "chart3_cpu_comparison.png"), dpi=150)
    plt.close(fig_mpl)
    print("  ✓ Chart 3 — CPU Comparison")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 4 — Memory Usage Comparison
# ─────────────────────────────────────────────────────────────────────────────

def chart_memory(grouped: dict, out_dir: str):
    cases = list(grouped.keys())
    peak_vals = [max(grouped[c], key=lambda r: r["num_records"])["peak_memory_mb"] for c in cases]
    avg_vals  = [max(grouped[c], key=lambda r: r["num_records"])["avg_memory_mb"]  for c in cases]
    labels    = [CASE_LABELS.get(c, c) for c in cases]
    colors    = [PLOTLY_COLORS.get(c, "#888") for c in cases]

    fig_html = go.Figure(data=[
        go.Bar(name="Peak Memory (MB)", x=labels, y=peak_vals, marker_color=colors, opacity=0.9),
        go.Bar(name="Avg Memory (MB)",  x=labels, y=avg_vals,  marker_color=colors, opacity=0.55),
    ])
    fig_html.update_layout(
        barmode="group",
        title="📊 Chart 4 — Memory Usage Comparison (at max record count)",
        yaxis_title="Memory (MB)",
        template="plotly_dark",
        font=dict(family="Inter, sans-serif", size=13),
    )
    fig_html.write_html(os.path.join(out_dir, "chart4_memory_comparison.html"))

    import numpy as np
    x = np.arange(len(labels))
    width = 0.35
    fig_mpl, ax = plt.subplots(figsize=(9, 6))
    ax.bar(x - width/2, peak_vals, width, label="Peak Memory (MB)", color=colors, alpha=0.9)
    ax.bar(x + width/2, avg_vals,  width, label="Avg Memory (MB)",  color=colors, alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel("Memory (MB)", fontsize=13)
    ax.set_title("Memory Usage Comparison", fontsize=15, fontweight="bold")
    ax.legend(fontsize=11)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    fig_mpl.tight_layout()
    fig_mpl.savefig(os.path.join(out_dir, "chart4_memory_comparison.png"), dpi=150)
    plt.close(fig_mpl)
    print("  ✓ Chart 4 — Memory Comparison")


# ─────────────────────────────────────────────────────────────────────────────
# Chart 5 — Speedup Ratio (Case 3 / Case 1) vs Record Count
# ─────────────────────────────────────────────────────────────────────────────

def chart_speedup(grouped: dict, out_dir: str):
    c1 = grouped.get("Case1_Sequential", [])
    c3 = grouped.get("Case3_Pipeline", [])
    if not c1 or not c3:
        print("  ⚠  Skipping Chart 5 — need both Case 1 and Case 3 data.")
        return

    # Build lookup by record count
    c1_map = {r["num_records"]: r["throughput_rec_s"] for r in c1}
    c3_map = {r["num_records"]: r["throughput_rec_s"] for r in c3}
    common_sizes = sorted(set(c1_map) & set(c3_map))
    speedups = [c3_map[n] / c1_map[n] for n in common_sizes]

    fig_html = go.Figure()
    fig_html.add_trace(go.Scatter(
        x=common_sizes, y=speedups, mode="lines+markers+text",
        text=[f"{s:.1f}×" for s in speedups],
        textposition="top center",
        line=dict(color="#8B5CF6", width=3),
        marker=dict(size=11, color="#8B5CF6"),
        name="Speedup (C3/C1)",
    ))
    fig_html.update_layout(
        title="📊 Chart 5 — Speedup Ratio (Case 3 Pipeline vs Case 1 Sequential)",
        xaxis_title="Number of Records",
        yaxis_title="Speedup Ratio (×)",
        template="plotly_dark",
        font=dict(family="Inter, sans-serif", size=13),
    )
    fig_html.write_html(os.path.join(out_dir, "chart5_speedup_ratio.html"))

    fig_mpl, ax = plt.subplots(figsize=(10, 6))
    ax.plot(common_sizes, speedups, "o-", color="#8B5CF6", linewidth=2.5, markersize=8)
    for n, s in zip(common_sizes, speedups):
        ax.annotate(f"{s:.1f}×", (n, s), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=10)
    ax.set_xlabel("Number of Records", fontsize=13)
    ax.set_ylabel("Speedup Ratio (×)", fontsize=13)
    ax.set_title("Speedup Ratio — Pipeline vs Sequential", fontsize=15, fontweight="bold")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    fig_mpl.tight_layout()
    fig_mpl.savefig(os.path.join(out_dir, "chart5_speedup_ratio.png"), dpi=150)
    plt.close(fig_mpl)
    print("  ✓ Chart 5 — Speedup Ratio")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def generate_all_charts(csv_path: str = DEFAULT_CSV, out_dir: str = CHARTS_DIR):
    store = ResultsStore(csv_path)
    rows = store.load_all()

    if not rows:
        print("⚠  No benchmark results found. Run benchmark_runner.py first.")
        return

    _ensure_output_dir()
    grouped = _group_by_case(rows)

    print(f"\nGenerating charts from {len(rows)} result rows…")
    chart_execution_time(grouped, out_dir)
    chart_throughput(grouped, out_dir)
    chart_cpu(grouped, out_dir)
    chart_memory(grouped, out_dir)
    chart_speedup(grouped, out_dir)

    print(f"\n✅ All charts saved to: {out_dir}/")
    print("   • chart1_execution_time.html/.png")
    print("   • chart2_throughput.html/.png")
    print("   • chart3_cpu_comparison.html/.png")
    print("   • chart4_memory_comparison.html/.png")
    print("   • chart5_speedup_ratio.html/.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DataFlux benchmark charts")
    parser.add_argument("--csv", type=str, default=DEFAULT_CSV, help="Path to benchmark_results.csv")
    parser.add_argument("--out", type=str, default=CHARTS_DIR, help="Output directory for charts")
    args = parser.parse_args()
    generate_all_charts(args.csv, args.out)
