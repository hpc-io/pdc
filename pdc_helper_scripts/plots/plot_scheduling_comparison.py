#!/usr/bin/env python3
"""
VPIC-IO dynamic-vs-static transformation scheduling comparison: one line
each for static and dynamic device placement, total transformation time
(H2D + compression + D2H, summed across every transformation run during
the workload) against node/process count, with the per-point percentage
speedup of dynamic over static labeled in green next to each pair.

Data: pdc_helper_scripts/external/data/sched/compression_times.csv
(cloned from https://github.com/TheAssembler1/data -- the original
source CSV for this figure; see that repo for provenance). Columns used
here: nodes, vpicio_static_total_s, vpicio_dynamic_total_s,
vpicio_speedup_pct. The bdcats_* columns in the same file are BDCATS's
own version of this sweep, not plotted here (VPIC-IO is this figure's
subject; see plot_curl_comparison.py/plot_magnitude_comparison.py's own
module docstrings for the analogous don't-mix-workloads reasoning).

Usage:
    python3 plot_scheduling_comparison.py [--csv FILE] [--out FILE.pdf]

Requires: numpy, matplotlib (no pandas).
"""
import argparse
import csv
import os

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Same reviewer-requested theme as plot_curl_comparison.py /
# plot_magnitude_comparison.py: PDF/PS output embeds only the 14 standard
# core fonts, and all text is rendered by a real LaTeX engine (needs
# latex, cm-super, and texlive-latex-extra's type1cm.sty installed).
plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

STATIC_COLOR = "#d62728"
DYNAMIC_COLOR = "#1f77b4"
SPEEDUP_COLOR = "#2ca02c"
RANKS_PER_NODE = 32  # same convention as every other figure in this paper


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_csv = os.path.normpath(os.path.join(script_dir, "..", "external", "data", "sched", "compression_times.csv"))
    ap.add_argument("--csv", default=default_csv, help="Path to compression_times.csv (default: external/data/sched/compression_times.csv)")
    ap.add_argument("--out", default=None, help="Output PDF path (default: <script dir>/scheduling_comparison.pdf)")
    args = ap.parse_args()

    out_path = args.out or os.path.join(script_dir, "scheduling_comparison.pdf")

    with open(args.csv, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit(f"No rows found in {args.csv!r}")

    nodes = [int(r["nodes"]) for r in rows]
    ranks = [n * RANKS_PER_NODE for n in nodes]
    static_s = [float(r["vpicio_static_total_s"]) for r in rows]
    dynamic_s = [float(r["vpicio_dynamic_total_s"]) for r in rows]
    speedup_pct = [float(r["vpicio_speedup_pct"]) for r in rows]

    x = np.arange(len(ranks))

    fig, ax = plt.subplots(figsize=(8.0, 4.0), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    ax.plot(x, static_s, marker="o", markersize=7, linewidth=1.8, color=STATIC_COLOR, label="Static", zorder=3)
    ax.plot(x, dynamic_s, marker="s", markersize=7, linewidth=1.8, color=DYNAMIC_COLOR, label="Dynamic", zorder=3)

    y_max = max(static_s)
    for xi, s, d, pct in zip(x, static_s, dynamic_s, speedup_pct):
        ax.text(xi, max(s, d) + y_max * 0.025, f"-{pct:.0f}\\%", ha="center", va="bottom",
                fontsize=11, fontweight="bold", color=SPEEDUP_COLOR, clip_on=False)

    ax.set_xticks(x)
    ax.set_xticklabels([str(r) for r in ranks], fontweight="bold")
    ax.set_xlabel("Number of Processes", fontsize=15, labelpad=8)
    ax.set_ylabel("Total Transformation Time (s)", fontsize=15)
    ax.tick_params(axis="y", labelsize=13)
    ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(0, y_max * 1.15)

    ax.legend(loc="upper left", fontsize=16, handlelength=2.5)

    fig.savefig(out_path, dpi=200, bbox_inches="tight", pad_inches=0.05)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
