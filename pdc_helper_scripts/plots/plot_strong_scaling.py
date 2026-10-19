#!/usr/bin/env python3
"""
Strong scaling of PDC data servers for the eager (in-flight) analysis
workloads: total workload time as a function of servers per node (2, 4, 8)
at each node count, one line per node count.

Same loaders and aggregation as plot_magnitude_comparison.py /
plot_curl_comparison.py, so every number here matches the comparison
charts. Two panels, one per analysis workload.

Excluded points: a run whose result CSV is incomplete (no close-server
timing recorded, write times far below the rest of its sweep) is dropped
rather than plotted. See EXCLUDED_RUNS below.

Usage:
    python3 plot_strong_scaling.py [--out FILE.png]

Requires: numpy, matplotlib (no pandas).
"""
import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import plot_magnitude_comparison as mag
import plot_curl_comparison as curl

HERE = os.path.dirname(os.path.abspath(__file__))
ANALYSIS = os.path.normpath(os.path.join(HERE, "..", "analysis"))

RANKS_PER_NODE = 32
SERVER_COUNTS = (2, 4, 8)

# (workload, servers_per_node, n_ranks) -> reason. Each entry is a run whose
# CSV is missing its close-server timing and whose write times are an order
# of magnitude below the rest of its sweep. Rerun it before re-including it.
EXCLUDED_RUNS = {
    ("magnitude", 8, 128): "job 58373748: avg_close_s recorded as 0, writes ~1.3 s vs ~8 s neighbors",
}

# Three representative weak-scaling points, so each line stays distinct:
# small (1 node), middle (16 nodes), and the largest (128 nodes). Colors are
# the Okabe-Ito colorblind-safe set. The dashed line is ideal strong scaling
# (time halves each time the server count doubles) anchored at the 2-server
# point of the 128-node run.
SHOW_NODES = (1, 16, 128)
SHOW_COLORS = {1: "#0072B2", 16: "#E69F00", 128: "#D55E00"}


def magnitude_totals():
    rows = mag.load_pdc_mode_all(os.path.join(ANALYSIS, "magnitude", "pdc"), "eager")
    out = {}
    for (ns, nr), r in rows.items():
        segs, _ = mag.aggregate("eager", r)
        out[(ns, nr)] = sum(segs.values())
    return out


def curl_totals():
    rows = curl.load_eager_all(os.path.join(ANALYSIS, "curl", "pdc"))
    out = {}
    for (ns, nr), r in rows.items():
        segs, _ = curl.aggregate(r, curl.EAGER_ONE_TIME, curl.EAGER_PER_STEP)
        out[(ns, nr)] = sum(segs.values())
    return out


def draw_panel(ax, totals, workload, title):
    totals = {k: v for k, v in totals.items() if (workload, k[0], k[1]) not in EXCLUDED_RUNS}
    for nodes in SHOW_NODES:
        nr = nodes * RANKS_PER_NODE
        xs = [ns for ns in SERVER_COUNTS if (ns, nr) in totals]
        ys = [totals[(ns, nr)] for ns in xs]
        if not xs:
            continue
        label = f"{nodes} node" + ("" if nodes == 1 else "s") + f" ({nr} ranks)"
        ax.plot(xs, ys, marker="o", markersize=9, linewidth=2.6, color=SHOW_COLORS[nodes],
                markeredgecolor="#222222", markeredgewidth=0.6, label=label, zorder=3)
    big = 128 * RANKS_PER_NODE
    if (2, big) in totals:
        ideal_x = np.array(SERVER_COUNTS, dtype=float)
        ideal_y = totals[(2, big)] * 2.0 / ideal_x
        ax.plot(ideal_x, ideal_y, linestyle="--", linewidth=1.8, color="#555555", zorder=2,
                label="ideal strong scaling (128 nodes)")
    ax.set_xscale("log", base=2)
    ax.set_xticks(SERVER_COUNTS)
    ax.set_xticklabels([str(n) for n in SERVER_COUNTS])
    ax.minorticks_off()
    ax.set_xlabel("PDC servers per node", fontsize=14)
    ax.set_ylabel("total workload time (s)", fontsize=14)
    ax.set_title(title, fontsize=15, fontweight="bold")
    ax.tick_params(labelsize=12)
    ax.grid(True, which="major", color="#bbbbbb", linewidth=0.8, alpha=0.7, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=12, loc="upper right")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=os.path.join(HERE, "strong_scaling.png"))
    args = ap.parse_args()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6.8), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    for ax in (ax1, ax2):
        ax.set_facecolor("#fcfcfb")
    draw_panel(ax1, magnitude_totals(), "magnitude", "Vector magnitude (eager)")
    draw_panel(ax2, curl_totals(), "curl", "Curl-vorticity-magnitude (eager)")
    fig.savefig(args.out, dpi=200, bbox_inches="tight")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
