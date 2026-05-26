#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parents[1] / "benchmarks/results/20260526-035952-benchmark-run/model-sweep/model_sweep.csv"
OUTPUT_PATH = CSV_PATH.with_name("model-sweep-tps-1to7.png")
TARGET_MIN = 1
TARGET_MAX = 7
PLOT_ONLY_OK = True


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def grouped_points(rows: list[dict[str, str]]) -> dict[str, list[tuple[int, float]]]:
    grouped: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for row in rows:
        if PLOT_ONLY_OK and row.get("status") != "ok":
            continue
        try:
            target = int(row["parallelism_target"])
        except Exception:
            continue
        if not (TARGET_MIN <= target <= TARGET_MAX):
            continue
        tps_raw = (row.get("tps") or "").strip()
        if not tps_raw:
            continue
        try:
            tps = float(tps_raw)
        except ValueError:
            continue
        grouped[row["series_label"]].append((target, tps))
    return {label: sorted(points) for label, points in grouped.items()}


def write_plot(path: Path, series: dict[str, list[tuple[int, float]]]) -> None:
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as exc:
        raise RuntimeError("matplotlib is required to generate the plot") from exc

    fig, ax = plt.subplots(figsize=(11, 6.5))
    for label, points in sorted(series.items()):
        xs, ys = zip(*points)
        ax.plot(xs, ys, marker="o", linewidth=2, label=label)
    ax.set_title(f"Model Sweep TPS ({TARGET_MIN}-{TARGET_MAX} Devices)")
    ax.set_xlabel("Parallelism Target")
    ax.set_ylabel("Tokens Per Second")
    ax.set_xlim(TARGET_MIN, TARGET_MAX)
    ax.set_xticks(list(range(TARGET_MIN, TARGET_MAX + 1)))
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def main() -> int:
    rows = load_rows(CSV_PATH)
    series = grouped_points(rows)
    if not series:
        raise RuntimeError(f"No plottable rows found in {CSV_PATH}")
    write_plot(OUTPUT_PATH, series)
    print(f"Wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
