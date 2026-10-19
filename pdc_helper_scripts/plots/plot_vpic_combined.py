#!/usr/bin/env python3
"""
VPIC-IO combined ZFP-transformation comparison: five series per node
count -- HDF5 Async+ZFP, PDC+ZFP on the CPU at two different background-
compute durations (40s and 100s of GEMM per timestep, see
evaluation.tex's "Overlapping Compute with I/O" discussion of why this
duration matters), PDC+ZFP on the GPU, and PDC+ZFP chained with
libsodium encryption. Each bar is a solid "4-step" segment (the first
four, overlappable timesteps) plus a hatched "outlier" segment on top
(the final, unoverlappable timestep) -- same convention
plot_vpicio_comparison.py's own write_main/write_outlier split and
plot_vpic_no_transform.py's port of no_transform/stacked.gnuplot use.

Data (all from https://github.com/TheAssembler1/data, cloned into
pdc_helper_scripts/external/):
    external/data/zfp_cpu/40_vs_100/vpic_zfp.csv
        ranks,avg_100s,std_100s,avg_40s,std_40s,outlier_100s,outlier_40s
        (avg_* is per-step; four_step_s = avg_*s * 4)
    external/data/zfp_gpu/vpic_joined_raw2.dat
        "<nodes> avg_data stdev_data avg_metadata stdev_metadata nodes outlier"
        (four_step_s = (avg_data + avg_metadata) * 4, same formula
        zfp_gpu_libsod's own precomputed "base" column confirms --
        zfp_gpu's own stacked.gnuplot uses column(3)=stdev_data instead
        of column(4)=avg_metadata in this same spot, which looks like a
        copy-paste bug against no_transform/stacked.gnuplot's original
        formula; this script uses the confirmed-correct (avg_data +
        avg_metadata) form instead of reproducing that bug)
    external/data/zfp_gpu_libsod/vpic_data_md_raw.csv
        ranks,avg_data,stdev_data,avg_metadata,stdev_metadata,base,outlier
        (base is already the precomputed four_step_s)
    external/data/zfp_gpu/hdf5_async_zfp_approx.csv
        nodes,four_step_s,outlier_s -- NOT real measured data, see that
        file's own header comment: the original HDF5 Async+ZFP source
        data isn't present anywhere in the external data repo (checked
        by filename and by grepping file contents), so these values were
        read off the existing in_paper/vpic_combined.pdf via pixel-level
        analysis against its calibrated y-axis gridlines, per explicit
        request. Replace with real data if/when it's found.

Usage:
    python3 plot_vpic_combined.py [--data-root DIR] [--out FILE.pdf]

Requires: numpy, matplotlib (no pandas).
"""
import argparse
import csv
import os

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Same reviewer-requested theme as plot_curl_comparison.py /
# plot_magnitude_comparison.py: PDF/PS output embeds only the 14 standard
# core fonts, and all text is rendered by a real LaTeX engine (needs
# latex, cm-super, and texlive-latex-extra's type1cm.sty installed).
plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

RANKS_PER_NODE = 32  # same convention as every other figure in this paper

# (series label, fill color) -- same five colors read off the original
# vpic_combined.pdf (HDF5 Async+ZFP orange, PDC ZFP CPU-40s green,
# PDC ZFP CPU-100s blue, PDC ZFP GPU purple, PDC ZFP+Encryption red).
SERIES_COLOR = {
    "HDF5 Async + ZFP": "#FF8F00",
    "DF ZFP (CPU, 40s)": "#66BB6A",
    "DF ZFP (CPU, 100s)": "#5C9BD6",
    "DF ZFP (GPU)": "#AB47BC",
    "DF ZFP + Encryption": "#EF5350",
}
SERIES_ORDER = list(SERIES_COLOR)


def load_hdf5_async(path):
    out = {}
    with open(path) as f:
        lines = [line for line in f if not line.lstrip().startswith("#")]
    for row in csv.DictReader(lines):
        out[int(row["nodes"])] = (float(row["four_step_s"]), float(row["outlier_s"]))
    return out


def load_cpu_40_100(path):
    """Returns (40s-dict, 100s-dict), each nodes -> (four_step_s, outlier_s)."""
    d40, d100 = {}, {}
    with open(path) as f:
        for row in csv.DictReader(f):
            n = int(row["ranks"])
            d40[n] = (float(row["avg_40s"]) * 4.0, float(row["outlier_40s"]))
            d100[n] = (float(row["avg_100s"]) * 4.0, float(row["outlier_100s"]))
    return d40, d100


def load_gpu_joined(path):
    """7-column whitespace .dat: nodes avg_data stdev_data avg_metadata
    stdev_metadata nodes outlier."""
    out = {}
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 7:
                continue
            nodes = int(float(parts[0]))
            avg_data, avg_metadata, outlier = float(parts[1]), float(parts[3]), float(parts[6])
            out[nodes] = ((avg_data + avg_metadata) * 4.0, outlier)
    return out


def load_gpu_libsod(path):
    out = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            out[int(row["ranks"])] = (float(row["base"]), float(row["outlier"]))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_data_root = os.path.normpath(os.path.join(script_dir, "..", "external", "data"))
    ap.add_argument("--data-root", default=default_data_root, help="Directory holding zfp_cpu/, zfp_gpu/, zfp_gpu_libsod/ (default: external/data)")
    ap.add_argument("--out", default=None, help="Output PDF path (default: <script dir>/vpic_combined.pdf)")
    args = ap.parse_args()

    out_path = args.out or os.path.join(script_dir, "vpic_combined.pdf")

    d40, d100 = load_cpu_40_100(os.path.join(args.data_root, "zfp_cpu", "40_vs_100", "vpic_zfp.csv"))
    per_series = {
        "HDF5 Async + ZFP": load_hdf5_async(os.path.join(args.data_root, "zfp_gpu", "hdf5_async_zfp_approx.csv")),
        "DF ZFP (CPU, 40s)": d40,
        "DF ZFP (CPU, 100s)": d100,
        "DF ZFP (GPU)": load_gpu_joined(os.path.join(args.data_root, "zfp_gpu", "vpic_joined_raw2.dat")),
        "DF ZFP + Encryption": load_gpu_libsod(os.path.join(args.data_root, "zfp_gpu_libsod", "vpic_data_md_raw.csv")),
    }

    all_nodes = sorted(set().union(*(d.keys() for d in per_series.values())))
    if not all_nodes:
        raise SystemExit(f"No data found under {args.data_root!r}")
    ranks = [n * RANKS_PER_NODE for n in all_nodes]

    n_groups = len(all_nodes)
    n_bars = len(SERIES_ORDER)
    group_width = 0.84
    slot_width = group_width / n_bars
    bar_width = slot_width * 0.88
    x = np.arange(n_groups)

    fig, ax = plt.subplots(figsize=(max(9.0, 1.6 * n_groups), 6.2), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    for bi, label in enumerate(SERIES_ORDER):
        color = SERIES_COLOR[label]
        offset = (bi - (n_bars - 1) / 2) * slot_width
        four_step = np.array([per_series[label].get(n, (0.0, 0.0))[0] for n in all_nodes])
        outlier = np.array([per_series[label].get(n, (0.0, 0.0))[1] for n in all_nodes])
        ax.bar(x + offset, four_step, bar_width, color=color,
               edgecolor="black", linewidth=0.6, zorder=3)
        ax.bar(x + offset, outlier, bar_width, bottom=four_step, color=color, hatch="///",
               edgecolor="black", linewidth=0.6, zorder=3)

    ax.set_xticks(x)
    ax.set_xticklabels([str(r) for r in ranks], fontweight="bold")
    ax.set_xlabel("Number of Processes", fontsize=15, labelpad=8)
    ax.set_ylabel("Observed I/O Time (s)", fontsize=15)
    ax.tick_params(axis="y", labelsize=13)
    ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    series_handles = [mpatches.Patch(facecolor=SERIES_COLOR[label], edgecolor="black") for label in SERIES_ORDER]
    leg1 = ax.legend(series_handles, SERIES_ORDER, loc="upper left", fontsize=12,
                     handlelength=2, ncol=2)
    ax.add_artist(leg1)

    seg_handles = [
        mpatches.Patch(facecolor="white", edgecolor="black"),
        mpatches.Patch(facecolor="white", hatch="///", edgecolor="black"),
    ]
    ax.legend(seg_handles, ["4-step", "Final-step"], loc="upper right", fontsize=12, handlelength=2)

    fig.savefig(out_path, dpi=200, bbox_inches="tight", pad_inches=0.05)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
