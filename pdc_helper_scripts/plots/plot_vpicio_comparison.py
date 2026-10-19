#!/usr/bin/env python3
"""
VPIC-IO write-throughput comparison: PDC raw (no transform), PDC + ZFP
CPU compression, PDC + ZFP GPU compression, PDC + ZFP GPU compression
chained with libsodium encryption -- each with a real per-server-count
(2/4/8) sweep, one bracket of bars plus a linear best-fit trend line per
rank count, same convention plot_magnitude_comparison.py /
plot_curl_comparison.py use for their own server-swept modes -- versus
ADIOS2 vpicio (no server, a single bar per rank count).

Only the writer's timing is compared. Every mode's own verification step
(PDC's vpicio_verify, ADIOS2's adios2_bdcats) runs as a genuinely
separate srun step after the writer fully exits and is never folded into
these numbers -- see vpicio.c's / vpicio_verify.c's / adios2_vpicio.cpp's
/ adios2_bdcats.cpp's own header comments for why verification time must
stay out of workload-timing comparisons.

Data layout this expects (each PDC variant is its own directory, since
they were reorganized by workload purpose, not by tool):

    pdc_helper_scripts/
      io/
        pdc_vpicio/results_vpicio_raw_<jobid>.csv           <- --pdc-raw-root
        adios2_vpicio/results_adios2_vpicio_<jobid>.csv     <- --adios2-root
                      results_adios2_bdcats_<jobid>.csv        (verification, not plotted)
      transformation/vpicio_zfp/                            <- --pdc-zfp-root
        results_zfp_compress_cpu_<jobid>.csv
        results_zfp_compress_gpu_<jobid>.csv
        results_zfp_gpu_then_encrypt_<jobid>.csv
      plots/plot_vpicio_comparison.py                        <- this file

Server count comes from each row's own servers_per_node column (every
current sbatch script emits it -- see each one's own sweep-loop header
comment), not from directory layout, so it doesn't matter which
<MM_DD_YYYY>-<jobid>/ output folder a file happens to live in; recursive
glob picks up any results_<variant>_*.csv anywhere under its root.

CSV schemas (see each sbatch script's own `echo "servers_per_node,..."`
header line):
    PDC variants (raw/zfp_compress_cpu/zfp_compress_gpu/
    zfp_gpu_then_encrypt): servers_per_node,mode,step,n_ranks,transform,
    setup_s,write_s,step_total_s,avg_close_s,total_with_close_s,
    last_step_write_s
    ADIOS2 (adios2_vpicio): servers_per_node,mode,step,n_ranks,setup_s,
    write_s,close_s,step_total_s,last_step_write_s

setup_s is one-time per job (meaned across a job's STEPS rows); write_s
is real per-timestep cost (summed). avg_close_s (PDC's close_server
checkpoint/flush RPC) and ADIOS2's own writer.Close() (close_s) are each
folded into their own "close" segment, separate from "write", same
close-is-a-real-distinct-cost rationale plot_magnitude_comparison.py's
module docstring explains -- not a footnote buried inside "write".

last_step_write_s is the write_s of the final (STEPS-th) timestep alone
-- VPIC-IO's real "outlier" I/O phase, which must run to completion
before the job can end and so can never overlap with a following step's
compute the way every earlier step can (evaluation.tex's own framing;
see each sbatch script's own comment on this column). Split out of the
summed write_s into its own write_outlier segment, drawn as the same
color as the non-outlier write_main segment but with a bolder hatch --
matching the paper's own "solid bar + hatched portion on top" convention
for this exact distinction, not a separately-colored cost category.

No particle-count column exists in any of these CSVs (unlike magnitude's
n_elem / curl's nx,ny,nz_per_rank), so unlike the other two plots this
one has no per-rank-count data-volume (GB) annotation under the x-axis
-- just the rank count itself. NPARTICLES is fixed per rank across the
whole sweep by convention (weak scaling; see each common.sh), so every
bar in a given rank-count group already represents the same data volume,
it's just not machine-checkable from the CSV alone.

Usage:
    python3 plot_vpicio_comparison.py [--pdc-raw-root DIR] [--pdc-zfp-root DIR] [--adios2-root DIR] [--out FILE.png]

Requires: numpy, matplotlib (no pandas).
"""
import argparse
import csv
import glob
import os
import re
from collections import defaultdict
from pathlib import Path

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import matplotlib.patches as mpatches
import matplotlib.transforms as mtransforms

# mode (== results_<mode>_*.csv filename stem) -> (one_time_cols, per_timestep_cols).
# last_step_write_s is a one-time value (same value repeated on every
# row of a sweep step, same convention as avg_close_s) -- the sbatch
# script already extracts it from the raw log's final STEPS-th row, so
# aggregate() doesn't need to re-derive "which row is last" itself.
_PDC_ONE_TIME = ["setup_s", "last_step_write_s"]
_PDC_PER_STEP = ["write_s"]
_ADIOS2_ONE_TIME = ["setup_s", "close_s", "last_step_write_s"]
_ADIOS2_PER_STEP = ["write_s"]

SCHEMAS = {
    "vpicio_raw": (_PDC_ONE_TIME, _PDC_PER_STEP),
    "zfp_compress_cpu": (_PDC_ONE_TIME, _PDC_PER_STEP),
    "zfp_compress_gpu": (_PDC_ONE_TIME, _PDC_PER_STEP),
    "zfp_gpu_then_encrypt": (_PDC_ONE_TIME, _PDC_PER_STEP),
    "adios2_vpicio": (_ADIOS2_ONE_TIME, _ADIOS2_PER_STEP),
}

# Raw CSV column -> one of three coarse cost segments. write_s and
# last_step_write_s are NOT listed here -- aggregate() handles that pair
# specially (the last step's own write_s, already summed into write_s
# once, has to be subtracted back out before splitting into
# write_main/write_outlier, which a simple 1:1 column->segment mapping
# can't express). setup_s counts toward write_main (a one-time,
# non-outlier job cost); avg_close_s/close_s stay their own segment, same
# as plot_magnitude_comparison.py's close segment.
RAW_TO_SEGMENT = {
    "setup_s": "write_main",
    "avg_close_s": "close",
    "close_s": "close",
}
# write_outlier is VPIC-IO's real "outlier" I/O phase -- the last of
# STEPS timesteps, which must run to completion before the job can end
# and so can never overlap with a following step's compute the way every
# earlier step can (see evaluation.tex's "the outlier phase" framing).
# Drawn as the same color as write_main but with a bolder hatch, matching
# the paper's own "hatched portion on top" convention for the identical
# concept, rather than a separately-colored segment.
SEGMENT_ORDER = ["write_main", "write_outlier", "close"]
SEGMENT_LABEL = {
    "write_main": "write",
    "write_outlier": "write, last step",
    "close": "close",
}
# Same write/close colors as plot_magnitude_comparison.py's sim_write/
# close segments, so the same cost category reads as the same color
# across all three comparison charts.
SEGMENT_COLOR = {"write_main": "#2a78d6", "write_outlier": "#2a78d6", "close": "#e87ba4"}
SEGMENT_HATCH = {"write_main": "..", "write_outlier": "///", "close": "||"}

# Each PDC variant gets its own 2/4/8-server bracket + trend line, same
# convention as plot_magnitude_comparison.py's MULTI_SERVER_MODES. Left
# to right: raw (baseline) first, then the three ZFP variants in the
# order they were added to transformation/vpicio_zfp/ (CPU, then GPU,
# then GPU+encrypt).
MULTI_SERVER_MODES = {
    "vpicio_raw": {"bracket_label": "PDC Raw", "colors": ["#0B3C5D", "#328CC1", "#89C2D9"]},
    "zfp_compress_cpu": {"bracket_label": "PDC ZFP CPU", "colors": ["#6A3D9A", "#9D6FC2", "#C9A6E0"]},
    "zfp_compress_gpu": {"bracket_label": "PDC ZFP GPU", "colors": ["#1B7A3D", "#4CAF6B", "#8FD4A3"]},
    "zfp_gpu_then_encrypt": {"bracket_label": "PDC ZFP GPU+Encrypt", "colors": ["#B03A2E", "#D9694F", "#F0A48B"]},
}
# Single-bar-per-rank-count mode (no server). Reuses
# plot_magnitude_comparison.py's / plot_curl_comparison.py's own ADIOS2
# color so the same method reads as the same color across all three
# charts.
MODE_ORDER = ["adios2_vpicio"]
MODE_LABEL = {"adios2_vpicio": "ADIOS2"}
MODE_COLOR = {"adios2_vpicio": "#5C6BC0"}
TREND_COLOR = "#52514e"


def read_results_csv(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return
        for row in reader:
            if len(row) != len(header):
                continue
            yield dict(zip(header, row))


SERVER_DIR_RE = re.compile(r"^(\d+)_servers")


def _servers_per_node_for_row(row, path):
    """Real server count for one CSV row -- the row's own servers_per_node
    column (every current sbatch script emits this) is the source of
    truth. Falls back to the nearest "<N>_servers*" ancestor directory
    name only for a row/file that somehow predates the column."""
    raw = row.get("servers_per_node", "")
    if raw not in ("", "FAILED"):
        try:
            return int(raw)
        except ValueError:
            pass
    for parent in Path(path).parents:
        m = SERVER_DIR_RE.match(parent.name)
        if m:
            return int(m.group(1))
    return None


def load_pdc_mode_all(results_root, mode):
    """(server_count, n_ranks) -> rows, searching recursively under
    results_root for results_<mode>_<jobid>.csv -- server count comes
    from each row's own servers_per_node column. Keeps the newest
    jobid's rows per (server_count, n_ranks) rerun."""
    name_re = re.compile(rf"^results_{re.escape(mode)}_(\d+)\.csv$")
    best = {}  # (n_servers, n_ranks) -> (jobid, rows)
    for path in glob.glob(os.path.join(results_root, "**", f"results_{mode}_*.csv"), recursive=True):
        m = name_re.match(os.path.basename(path))
        if not m:
            continue
        jobid = int(m.group(1))
        rows_by_key = defaultdict(list)
        for row in read_results_csv(path):
            try:
                n_ranks = int(row["n_ranks"])
            except (KeyError, ValueError):
                continue
            n_servers = _servers_per_node_for_row(row, path)
            if n_servers is None:
                continue
            rows_by_key[(n_servers, n_ranks)].append(row)
        for key, rows in rows_by_key.items():
            if key not in best or jobid > best[key][0]:
                best[key] = (jobid, rows)
    return {key: rows for key, (_, rows) in best.items()}


def load_mode_anywhere(results_root, mode):
    """n_ranks -> rows, searching recursively under results_root for
    results_<mode>_<jobid>.csv, keeping only the newest jobid's rows per
    n_ranks. Used for adios2_vpicio (servers_per_node is always 0, so no
    (server, n_ranks) breakout makes sense)."""
    name_re = re.compile(rf"^results_{re.escape(mode)}_(\d+)\.csv$")
    best = {}  # n_ranks -> (jobid, rows)
    for path in glob.glob(os.path.join(results_root, "**", f"results_{mode}_*.csv"), recursive=True):
        m = name_re.match(os.path.basename(path))
        if not m:
            continue
        jobid = int(m.group(1))
        rows_by_ranks = defaultdict(list)
        for row in read_results_csv(path):
            try:
                n_ranks = int(row["n_ranks"])
            except (KeyError, ValueError):
                continue
            rows_by_ranks[n_ranks].append(row)
        for n_ranks, rows in rows_by_ranks.items():
            if n_ranks not in best or jobid > best[n_ranks][0]:
                best[n_ranks] = (jobid, rows)
    return {n_ranks: rows for n_ranks, (_, rows) in best.items()}


def aggregate(mode, rows):
    """Reconstruct one job's true total write time, folded into three
    coarse segments (write_main, write_outlier, close). One-time columns
    (setup_s, avg_close_s/close_s, last_step_write_s) meaned across a
    job's timestep rows; write_s summed. Returns segments: dict[seg] ->
    seconds."""
    one_time_cols, per_step_cols = SCHEMAS[mode]
    raw = {}
    for col in one_time_cols:
        vals = [float(r[col]) for r in rows if r.get(col, "") not in ("", "FAILED")]
        raw[col] = float(np.mean(vals)) if vals else 0.0
    for col in per_step_cols:
        vals = [float(r[col]) for r in rows if r.get(col, "") not in ("", "FAILED")]
        raw[col] = float(np.sum(vals))

    # avg_close_s: one-time per job, PDC-backed modes only (adios2_vpicio
    # has its own close_s handled via SCHEMAS instead).
    close_vals = [float(r["avg_close_s"]) for r in rows if r.get("avg_close_s", "") not in ("", "FAILED")]
    if close_vals:
        raw["avg_close_s"] = float(np.mean(close_vals))

    # write_s (summed across every step, including the last) and
    # last_step_write_s (that last step's own write_s, one-time) aren't
    # in RAW_TO_SEGMENT -- split them here instead: write_outlier is the
    # last step alone, write_main is every other step's write_s (the
    # summed total minus the outlier, so it isn't double-counted).
    total_write_s = raw.pop("write_s", 0.0)
    write_outlier = raw.pop("last_step_write_s", 0.0)
    write_main = max(0.0, total_write_s - write_outlier)

    folded = {seg: 0.0 for seg in SEGMENT_ORDER}
    folded["write_main"] += write_main
    folded["write_outlier"] += write_outlier
    for col, val in raw.items():
        folded[RAW_TO_SEGMENT[col]] += val
    return folded


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_pdc_raw_root = os.path.normpath(os.path.join(script_dir, "..", "io", "pdc_vpicio"))
    default_pdc_zfp_root = os.path.normpath(os.path.join(script_dir, "..", "transformation", "vpicio_zfp"))
    default_adios2_root = os.path.normpath(os.path.join(script_dir, "..", "io", "adios2_vpicio"))
    ap.add_argument("--pdc-raw-root", default=default_pdc_raw_root, help="Directory holding results_vpicio_raw_*.csv (default: io/pdc_vpicio)")
    ap.add_argument("--pdc-zfp-root", default=default_pdc_zfp_root, help="Directory holding results_zfp_*_*.csv (default: transformation/vpicio_zfp)")
    ap.add_argument("--adios2-root", default=default_adios2_root, help="Directory holding results_adios2_vpicio_*.csv (default: io/adios2_vpicio)")
    ap.add_argument("--out", default=None, help="Output PNG path (default: <script dir>/vpicio_comparison.png)")
    args = ap.parse_args()

    out_path = args.out or os.path.join(script_dir, "vpicio_comparison.png")

    mode_root = {
        "vpicio_raw": args.pdc_raw_root,
        "zfp_compress_cpu": args.pdc_zfp_root,
        "zfp_compress_gpu": args.pdc_zfp_root,
        "zfp_gpu_then_encrypt": args.pdc_zfp_root,
    }

    per_mode_rows = {ms_mode: load_pdc_mode_all(root, ms_mode) for ms_mode, root in mode_root.items()}
    server_counts = sorted({n_servers for rows in per_mode_rows.values() for (n_servers, _) in rows})
    if not server_counts:
        raise SystemExit(
            f"No results_<variant>_*.csv rows found under {args.pdc_raw_root!r} or {args.pdc_zfp_root!r} -- "
            "run the sweeps first (see each sbatch script's own header comment)."
        )
    print(f"Found server counts: {server_counts} (applies to raw + all three ZFP variants)")

    # series key -> n_ranks -> segments
    per_series = {}
    for ms_mode, rows_by_key in per_mode_rows.items():
        for n_servers in server_counts:
            data = {
                n_ranks: aggregate(ms_mode, rows)
                for (ns, n_ranks), rows in rows_by_key.items() if ns == n_servers
            }
            if data:
                per_series[(ms_mode, n_servers)] = data

    if not os.path.isdir(args.adios2_root):
        print(f"NOTE: {args.adios2_root!r} does not exist -- skipping ADIOS2")
    else:
        rows_by_ranks = load_mode_anywhere(args.adios2_root, "adios2_vpicio")
        segs_by_ranks = {n_ranks: aggregate("adios2_vpicio", rows) for n_ranks, rows in rows_by_ranks.items()}
        if not segs_by_ranks:
            print(f"NOTE: no results_adios2_vpicio_*.csv data found under {args.adios2_root!r} -- skipping")
        else:
            per_series[("adios2_vpicio", None)] = segs_by_ranks

    # Bar order left to right: each MULTI_SERVER_MODES entry's own
    # 2/4/8-server bars in turn (raw, zfp_cpu, zfp_gpu,
    # zfp_gpu_then_encrypt), then ADIOS2 last.
    multi_series = [(ms_mode, n) for ms_mode in MULTI_SERVER_MODES for n in server_counts if (ms_mode, n) in per_series]
    single_series = [(m, None) for m in MODE_ORDER if (m, None) in per_series]
    series = multi_series + single_series
    if not series:
        raise SystemExit("No results_<variant>_*.csv rows found for any mode")

    all_ranks = sorted({n for segs in per_series.values() for n in segs})

    def bar_label(key):
        mode, n_servers = key
        return str(n_servers) if mode in MULTI_SERVER_MODES else MODE_LABEL[mode]

    def bar_label_color(key):
        mode, n_servers = key
        if mode in MULTI_SERVER_MODES:
            colors = MULTI_SERVER_MODES[mode]["colors"]
            return colors[server_counts.index(n_servers) % len(colors)]
        return MODE_COLOR[mode]

    n_groups = len(all_ranks)
    n_bars = len(series)
    group_width = 0.74

    # slot_width has to be solved for accounting for the cluster-boundary
    # gaps too: n_bars slots + n_transitions gaps must sum to group_width,
    # not group_width plus however many gaps happen to be needed --
    # otherwise more clusters (or a wider GAP_RATIO) silently pushes the
    # whole group past the 1.0 spacing between rank-count groups and bars
    # start overlapping their neighboring group's bars instead of just
    # each other. Same fix as plot_magnitude_comparison.py's identical bug.
    def cluster_of(key):
        return key[0] if key[0] in MULTI_SERVER_MODES else "single"

    n_transitions = sum(1 for a, b in zip(series, series[1:]) if cluster_of(a) != cluster_of(b))
    GAP_RATIO = 2.5
    slot_width = group_width / max(n_bars + n_transitions * GAP_RATIO, 1)
    bar_width = slot_width * 0.8
    x = np.arange(n_groups)

    extra_gap = slot_width * GAP_RATIO
    offsets, pos, prev_cluster = [], 0.0, None
    for key in series:
        c = cluster_of(key)
        if prev_cluster is not None and c != prev_cluster:
            pos += extra_gap
        offsets.append(pos)
        pos += slot_width
        prev_cluster = c
    offsets = np.array(offsets) - np.mean(offsets)

    # Width scales with both rank-count groups and bars/group -- up to 4
    # server-swept modes x 3 servers + 1 ADIOS2 bar = 13 bars/group here,
    # denser than magnitude's/curl's 4-5, so this uses a per-bar inch
    # budget (~0.22in/bar) rather than magnitude's/curl's flat
    # per-group factor, which would make a 13-bar group far too wide.
    fig, ax = plt.subplots(figsize=(max(11.0, 0.22 * n_groups * n_bars), 8.5), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    bar_centers = {}  # (key, group_index) -> x position
    for bi, key in enumerate(series):
        offset = offsets[bi]
        bottoms = np.zeros(n_groups)
        for seg in SEGMENT_ORDER:
            heights = np.array([per_series[key].get(n_ranks, {}).get(seg, 0.0) for n_ranks in all_ranks])
            if not np.any(heights > 0):
                continue
            ax.bar(
                x + offset, heights, bar_width, bottom=bottoms,
                color=SEGMENT_COLOR[seg], hatch=SEGMENT_HATCH[seg],
                edgecolor="white", linewidth=0.6, zorder=3,
            )
            bottoms += heights
        for gi, n_ranks in enumerate(all_ranks):
            bar_centers[(key, gi)] = x[gi] + offset

    y_max = max((sum(per_series[k][n].values()) for k in series for n in per_series[k]), default=1.0)
    # Extra headroom so the leftmost group's bracket, which sits right
    # where the (large, per explicit request) "cost segment" legend is
    # anchored, has enough vertical clearance not to render underneath
    # (and visually washed out by) the legend box.
    ax.set_ylim(0, y_max * 1.8)

    # Strong-scaling bracket + two-line header above each
    # MULTI_SERVER_MODES cluster's 3 server-count bars, same convention
    # as plot_magnitude_comparison.py.
    tick_h = y_max * 0.012
    for ms_mode, info in MULTI_SERVER_MODES.items():
        for gi, n_ranks in enumerate(all_ranks):
            keys = [(ms_mode, n) for n in server_counts if (ms_mode, n) in per_series and n_ranks in per_series[(ms_mode, n)]]
            if not keys:
                continue
            xs = [bar_centers[(k, gi)] for k in keys]
            local_max = max(sum(per_series[k][n_ranks].values()) for k in keys)
            bracket_y = local_max + y_max * 0.025
            x0, x1 = min(xs) - bar_width / 2, max(xs) + bar_width / 2
            color = info["colors"][0]
            ax.plot([x0, x0, x1, x1], [bracket_y - tick_h, bracket_y, bracket_y, bracket_y - tick_h],
                    color=color, linewidth=1.1, zorder=6, clip_on=False)
            ax.text((x0 + x1) / 2, bracket_y + y_max * 0.012, info["bracket_label"],
                    ha="center", va="bottom", fontsize=9.5, fontweight="bold", color=color, linespacing=1.3)

    # Linear best-fit line through each rank-count group's 2/4/8-server
    # totals, once per MULTI_SERVER_MODES entry.
    trend_label_used = False
    for ms_mode in MULTI_SERVER_MODES:
        for gi, n_ranks in enumerate(all_ranks):
            xs, ys = [], []
            for n_servers in server_counts:
                key = (ms_mode, n_servers)
                if key in per_series and n_ranks in per_series[key]:
                    xs.append(bar_centers[(key, gi)])
                    ys.append(sum(per_series[key][n_ranks].values()))
            if len(xs) < 2:
                continue
            coeffs = np.polyfit(xs, ys, 1)
            fit_xs = np.linspace(min(xs), max(xs), 20)
            fit_ys = np.polyval(coeffs, fit_xs)
            ax.plot(
                fit_xs, fit_ys, linestyle="--", linewidth=1.6, color=TREND_COLOR, zorder=4, alpha=0.85,
                label="linear fit (per server-swept variant, per rank count)" if not trend_label_used else None,
            )
            trend_label_used = True

    ax.set_xticks(x)
    ax.set_xticklabels([str(n) for n in all_ranks])
    ax.set_xlabel("Number of Processes", fontsize=13)
    ax.set_ylabel("total workload time (s)", fontsize=13)
    ax.tick_params(axis="y", labelsize=11)
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
    ax.yaxis.grid(True, which="minor", linestyle="-", linewidth=0.5, color="#aaaaaa", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Font scales with bar density -- see
    # plot_magnitude_comparison.py's identical bar_label_fontsize for why
    # a fixed size can't work across charts with different bar
    # counts/group (this chart alone ranges from ~4 bars today to ~13
    # once all four PDC variants plus ADIOS2 have real data).
    bar_label_fontsize = float(np.clip(95.0 / max(n_bars, 1), 8.0, 14.0))
    label_trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)
    for key in series:
        for gi, n_ranks in enumerate(all_ranks):
            if n_ranks not in per_series[key]:
                continue
            ax.text(
                bar_centers[(key, gi)], -0.02, bar_label(key),
                transform=label_trans, rotation=90, ha="center", va="top",
                fontsize=bar_label_fontsize, fontweight="bold", color=bar_label_color(key), clip_on=False,
            )
    ax.tick_params(axis="x", pad=88, labelsize=10.5, labelcolor="#111111")

    group_line_trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)
    for gi, n_ranks in enumerate(all_ranks):
        group_keys = [k for k in series if n_ranks in per_series[k]]
        if not group_keys:
            continue
        xs = [bar_centers[(k, gi)] for k in group_keys]
        ax.plot(
            [min(xs) - bar_width / 2, max(xs) + bar_width / 2], [-0.195, -0.195],
            transform=group_line_trans, color="#999999", linewidth=1.0, clip_on=False, zorder=6,
        )

    seg_keys = [s for s in SEGMENT_ORDER if any(per_series[k].get(n, {}).get(s, 0.0) > 0 for k in series for n in all_ranks)]
    seg_handles = [mpatches.Patch(facecolor=SEGMENT_COLOR[s], hatch=SEGMENT_HATCH[s], edgecolor="white") for s in seg_keys]
    seg_labels = [SEGMENT_LABEL[s] for s in seg_keys]
    leg1 = ax.legend(seg_handles, seg_labels, loc="upper left", fontsize=14, handlelength=3, handleheight=2.2)
    ax.add_artist(leg1)

    if trend_label_used:
        trend_handle = plt.Line2D([0], [0], linestyle="--", linewidth=1.6, color=TREND_COLOR)
        ax.legend([trend_handle], ["linear fit (per server-swept variant, per rank count)"], loc="upper right", fontsize=14, handlelength=3)

    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
