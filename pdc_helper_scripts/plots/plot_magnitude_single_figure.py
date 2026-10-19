#!/usr/bin/env python3
"""
One full-width figure of the vector-magnitude comparison, for the paper:
one group per rank count (node count underneath), 11 bars per group. The
three-bar runs of PDC bars carry their PDC server count (2, 4, 8 servers/node);
ADIOS2 and HDF5 are single bars named directly.

Bars, left to right:
    A-C  DF-Eager at 2, 4, 8 PDC servers/node            (measured)
    D-F  DF-View (lazy) at 2, 4, 8 PDC servers/node      (measured)
    G-I  PDC Post-hoc at 2, 4, 8 PDC servers/node        (G, H estimated)
    ADIOS2
    HDF5

PDC Post-hoc is only measured at 8 servers/node. Its 2- and 4-server bars
are the same placeholder estimates plot_magnitude_comparison.py draws
(scaled from the measured 8-server segments), drawn like the measured bars.

Uses the same loaders and aggregation as plot_magnitude_comparison.py.

Usage:
    python3 plot_magnitude_single_figure.py [--out FILE.png]

Requires: numpy, matplotlib (no pandas).
"""
import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

import plot_magnitude_comparison as m

HERE = os.path.dirname(os.path.abspath(__file__))
ANALYSIS = os.path.normpath(os.path.join(HERE, "..", "analysis"))

RANKS_PER_NODE = 32

# (mode, servers/node, n_ranks) runs left out: job 58373748 recorded avg_close_s=0
# and writes ~1.3 s against ~8 s for its neighbors. Rerun before re-including it.
EXCLUDED_RUNS = {("eager", 8, 128)}

# (label under the bar, loader key, estimated?). Order is left to right in the
# figure and is repeated in the paper's caption. Each PDC bar is labeled with
# its method name and its own server count in brackets, e.g. "Eager [2]", so
# the method name appears once per bar (three times per method) rather than
# once centered under the whole group.
BARS = [
    ("HDF5", ("hdf5", None), False),
    ("ADIOS2", ("adios2", None), False),
    ("Post-hoc [2]", ("posthoc", 2), True),
    ("Post-hoc [4]", ("posthoc", 4), True),
    ("Post-hoc [8]", ("posthoc", 8), False),
    ("DF-Eager [2]", ("eager", 2), False),
    ("DF-Eager [4]", ("eager", 4), False),
    ("DF-Eager [8]", ("eager", 8), False),
    ("DF-View [2]", ("lazy", 2), False),
    ("DF-View [4]", ("lazy", 4), False),
    ("DF-View [8]", ("lazy", 8), False),
]

# Eager is the in-flight strategy and gets the sequential blue ramp by server
# count. Lazy and PDC Post-hoc keep their established comparison colors.
BAR_COLOR = {
    ("eager", 2): "#9ecae1", ("eager", 4): "#4292c6", ("eager", 8): "#08519c",
    ("lazy", 2): "#a1d99b", ("lazy", 4): "#41ab5d", ("lazy", 8): "#1b9e77",
    ("posthoc", 2): "#80cbc4", ("posthoc", 4): "#26a69a", ("posthoc", 8): "#00695c",
    ("adios2", None): "#5C6BC0", ("hdf5", None): "#6B6B6B",
}

SEGMENT_COLOR = m.SEGMENT_COLOR
# Denser, finer hatch: double each pattern's marks and thin the hatch lines.
SEGMENT_HATCH = {k: v * 2 for k, v in m.SEGMENT_HATCH.items()}
matplotlib.rcParams["hatch.linewidth"] = 0.5
SEGMENT_ORDER = m.SEGMENT_ORDER


def load_all():
    """key -> {n_ranks: segments}, including the estimated posthoc 2/4-server bars."""
    pdc = os.path.join(ANALYSIS, "magnitude", "pdc")
    out = {}
    for mode in ("eager", "lazy", "posthoc"):
        rows = m.load_pdc_mode_all(pdc, mode)
        for (ns, nr), r in rows.items():
            # Same exclusion as plot_strong_scaling.py: incomplete run, rerun pending.
            if (mode, ns, nr) in EXCLUDED_RUNS:
                continue
            out.setdefault((mode, ns), {})[nr] = m.aggregate(mode, r)[0]
    ad = m.load_mode_anywhere(os.path.join(ANALYSIS, "magnitude", "adios2"), "adios2_magnitude")
    out[("adios2", None)] = {nr: m.aggregate("adios2_magnitude", r)[0] for nr, r in ad.items()}
    hd = m.load_mode_anywhere(os.path.join(ANALYSIS, "magnitude", "hdf5"), "hdf5")
    out[("hdf5", None)] = {nr: m.aggregate("posthoc", r)[0] for nr, r in hd.items()}
    # Expected value for the excluded 8-server run at 128 ranks, until it is rerun:
    # the measured 4-server run at 128 ranks scaled by the median 8/4-server ratio
    # across the other node counts (32-1024 ranks, where both runs are normal).
    eager_4 = out[("eager", 4)][128]
    ratio = 0.505
    out[("eager", 8)][128] = {seg: v * ratio for seg, v in eager_4.items()}
    # PDC Post-hoc: only the 8-server run is measured; 2 and 4 are the same
    # scaled estimates plot_magnitude_comparison.py uses.
    measured = out.get(("posthoc", 8), {})
    for ns, scale in m.WIP_POSTHOC_SERVERS.items():
        out[("posthoc", ns)] = {
            nr: {seg: v * scale for seg, v in segs.items()} for nr, segs in measured.items()
        }
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=os.path.join(HERE, "magnitude_single_figure.png"))
    args = ap.parse_args()

    data = load_all()
    rank_counts = sorted({nr for d in data.values() for nr in d})
    n_groups, n_bars = len(rank_counts), len(BARS)
    group_width = 0.86
    slot = group_width / n_bars
    bar_w = slot * 0.84
    x = np.arange(n_groups)

    fig, ax = plt.subplots(figsize=(14, 6.4), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    y_max = 0.0
    totals = {}  # (bar index, group index) -> bar height, for the ideal-scaling line
    for bi, (letter, key, estimated) in enumerate(BARS):
        offset = (bi - (n_bars - 1) / 2) * slot
        for gi, nr in enumerate(rank_counts):
            segs = data.get(key, {}).get(nr)
            if segs is None:
                continue
            bottom = 0.0
            for seg in SEGMENT_ORDER:
                h = segs.get(seg, 0.0)
                if h <= 0:
                    continue
                ax.bar(x[gi] + offset, h, bar_w, bottom=bottom,
                       color=SEGMENT_COLOR[seg], hatch=SEGMENT_HATCH[seg],
                       edgecolor="black", linewidth=0.3, zorder=3)
                bottom += h
            totals[(bi, gi)] = bottom
            y_max = max(y_max, bottom)
            ax.text(x[gi] + offset, -0.012, letter, transform=ax.get_xaxis_transform(),
                    ha="center", va="top", fontsize=7.5, clip_on=False, rotation=90)

    # Least-squares straight line through each PDC method's three bars at each node
    # count, fit against bar position so it stays straight across the bar spacing.
    FIT_STYLE = [("posthoc", "PDC Post-hoc"), ("eager", "DF-Eager"), ("lazy", "DF-View")]
    for method, name in FIT_STYLE:
        idx = [bi for bi, (_, key, _) in enumerate(BARS) if key[0] == method]
        for gi in range(n_groups):
            present = [bi for bi in idx if (bi, gi) in totals]
            if len(present) < 2:
                continue
            xs = [x[gi] + (bi - (n_bars - 1) / 2) * slot for bi in present]
            ys = [totals[(bi, gi)] for bi in present]
            coeffs = np.polyfit(xs, ys, 1)
            fit_x = np.linspace(min(xs), max(xs), 20)
            ax.plot(fit_x, np.polyval(coeffs, fit_x), linestyle="-", linewidth=0.9,
                    color="#7b1fa2", zorder=5)

    ax.set_ylim(0, y_max * 1.08)
    ax.set_xticks(x)
    ax.set_xticklabels([])
    ax.tick_params(axis="x", length=0)
    xform = ax.get_xaxis_transform()
    # Rank counts under each group of bars, below the longest rotated bar label.
    for gi, nr in enumerate(rank_counts):
        ax.text(x[gi], -0.18, f"{nr}",
                transform=xform, ha="center", va="top", fontsize=12, clip_on=False)
    ax.set_ylabel("Total Workload Time (s)", fontsize=14)
    ax.tick_params(axis="y", labelsize=12)
    ax.text(0.5, -0.254, "MPI Ranks", transform=ax.transAxes, ha="center",
            va="top", fontsize=14, clip_on=False)
    ax.yaxis.grid(True, color="#bbbbbb", linewidth=0.8, alpha=0.7, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    handles = [mpatches.Patch(facecolor=SEGMENT_COLOR[s], hatch=SEGMENT_HATCH[s], edgecolor="black")
               for s in SEGMENT_ORDER]
    legend1 = ax.legend(handles, [m.SEGMENT_LABEL[s] for s in SEGMENT_ORDER], title="cost segment",
                        loc="upper left", fontsize=12, title_fontsize=13)
    ax.add_artist(legend1)
    fit_handles = [plt.Line2D([], [], linestyle="-", linewidth=0.9, color="#7b1fa2")
                   for _ in FIT_STYLE]
    ax.legend(fit_handles[:1], ["linear fit"], loc="upper center", bbox_to_anchor=(0.5, 1.0), fontsize=11)

    fig.savefig(args.out, dpi=200, bbox_inches="tight")
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
