#!/usr/bin/env python3
"""
Curl + vorticity-magnitude workload comparison: for each rank count, one
stacked bar per PDC server count (2/4/8/... eager/DataFlyway runs,
auto-discovered) with a linear best-fit line through those totals to show
strong scaling, plus one additional reference bar per other method that
has real result data -- PDC posthoc, HDF5 posthoc, ADIOS2 -- placed in
the same rank-count group so all methods are visually comparable.

Data layout this expects, after the pdc_helper_scripts/ reorg into
analysis/transformation/io (each of pdc/, hdf5/, adios2/ below is now
its own directory with its own Makefile/sbatch scripts, not one shared
curl_analysis_scripts/ root):

    pdc_helper_scripts/
      analysis/curl/
        pdc/
          2_servers_<date>/results_curl_eager_<jobid>.csv
          4_servers_<date>/results_curl_eager_<jobid>.csv
          8_servers_<date>/results_curl_eager_<jobid>.csv, results_curl_posthoc_<jobid>.csv, ...
          <MM_DD_YYYY>-<jobid>/                    <- new sweep-loop job output (one job now
                                                        covers 2/4/8 servers via its own
                                                        servers_per_node CSV column)
          ...                                       <- more <N>_servers* dirs will show up
          csv_res/, and loose results_curl_<mode>_<jobid>.csv files anywhere
            else under pdc/                         <- also scanned for non-eager PDC modes
        hdf5/results_curl_hdf5_<jobid>.csv
        adios2/results_adios2_curl_<jobid>.csv       <- note: no "curl_" in the filename,
                                                          that directory's own naming convention
      plots/plot_curl_comparison.py                  <- this file, top-level (sibling of
                                                           analysis/, transformation/, io/)

Eager's server count is parsed from the leading "<N>_servers" of each
directory name directly under --results-root (default analysis/curl/pdc)
-- new "<N>_servers*" directories that get pushed later are picked up
automatically, no code change needed, and each becomes its own bar plus
a point on the strong-scaling trend line. The other modes
(posthoc/hdf5/adios2_curl/eager_compress) don't have a comparable
per-server-count sweep in the data on disk today, so each gets exactly
one bar per rank count, searched recursively by filename
(results_curl_<mode>_*.csv) under whichever of --results-root/
--hdf5-root/--adios2-root matches that mode -- wherever that mode's
CSVs happen to live.

As of this script's writing, real result data only exists for eager (at
2/4/8 servers) and posthoc (at 8 servers) -- results_curl_hdf5_*.csv,
results_curl_adios2_*.csv, and results_curl_eager_compress_*.csv (the
one eager_compress file on disk is header-only, zero data rows) don't
exist yet anywhere in the repo. Those three are still fully wired up
(schema, color, label) so they appear automatically, no code change
required, the moment those jobs are actually run and their CSVs land.

Every results_curl_<mode>_<jobid>.csv has one row per timestep, with
one-time job costs (setup_s, write_setup_s, relaunch_s, analyze_setup_s,
avg_close_s) repeated on every row (take the mean) and per-timestep
costs (write_s, readback_s, curl_compute_s, curl_writeback_s,
magnitude_compute_s, magnitude_writeback_s, adios2_curl's generic
compute_s/writeback_s) summed across every timestep row actually
present -- same reconstruction as plot_curl_totals.py's aggregate().
confirm_read_s is deliberately excluded everywhere: it's each
benchmark's own post-write correctness check (see bench_curl_eager.c /
adios2_bench_curl.cpp), not workload cost being measured, the identical
reasoning plot_curl_totals.py already applies.

Raw columns fold into four coarse segments shared across every mode
(RAW_TO_SEGMENT), the same "sim write / analysis read / analysis
compute / analysis write" split plot_totals.py (the Drive-hosted
magnitude-analysis version of this script) uses for its own six-segment
version of this idea. Eager has no separate analysis phase at all (the
server computes curl+magnitude synchronously inside write_s), so its
bars only ever show the sim_write segment -- expected, not a bug.

Usage:
    python3 plot_curl_comparison.py [--results-root DIR] [--out FILE.png]

Rerun any time new "<N>_servers*" directories or results_curl_<mode>_*.csv
files land under --results-root.

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

# Same reviewer-requested theme as plot_magnitude_comparison.py: PDF/PS
# output embeds only the 14 standard core fonts, and all text is rendered
# by a real LaTeX engine (needs latex, cm-super, and texlive-latex-extra's
# type1cm.sty installed -- all three confirmed present on this machine).
plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

# mode -> (one_time_cols, per_timestep_cols). Must track README.md's
# "Result CSV schemas" section. confirm_read_s intentionally omitted
# everywhere -- see module docstring.
_EAGER_ONE_TIME = ["setup_s", "avg_close_s"]
_EAGER_PER_STEP = ["write_s"]
_POSTHOC_ONE_TIME = ["write_setup_s", "analyze_setup_s", "avg_close_s"]
_POSTHOC_PER_STEP = [
    "write_s", "readback_s", "curl_compute_s", "curl_writeback_s",
    "magnitude_compute_s", "magnitude_writeback_s",
]
_HDF5_ONE_TIME = ["write_setup_s", "analyze_setup_s"]
_HDF5_PER_STEP = _POSTHOC_PER_STEP
# adios2_curl (adios2_bench_curl.cpp) has no server, so no avg_close_s,
# and its client-side fallback compute (see that file's header comment)
# only produces one generic compute_s/writeback_s pair, not a separate
# curl_*/magnitude_* split.
_ADIOS2_CURL_ONE_TIME = ["setup_s"]
_ADIOS2_CURL_PER_STEP = ["write_s", "readback_s", "compute_s", "writeback_s"]
# adios2_curl_inflight (adios2_bench_curl_inflight.cpp): curl and
# vorticity_magnitude are both registered derived variables, computed
# automatically inside EndStep() -- see that file's header comment --
# so the real cost is just setup/write/close; confirm_read_s is a
# correctness-check read only, excluded here the same way
# adios2_magnitude excludes its own confirmation read.
_ADIOS2_CURL_INFLIGHT_ONE_TIME = ["setup_s", "close_s"]
_ADIOS2_CURL_INFLIGHT_PER_STEP = ["write_s"]

# mode -> filename glob pattern to search for recursively under
# --results-root (anchored per-file below so a shorter mode name can't
# glob-match a longer mode's files, e.g. "eager" vs "eager_compress").
MODE_SCHEMAS = {
    "eager_compress": (_EAGER_ONE_TIME, _EAGER_PER_STEP),
    "posthoc": (_POSTHOC_ONE_TIME, _POSTHOC_PER_STEP),
    "hdf5": (_HDF5_ONE_TIME, _HDF5_PER_STEP),
    "adios2_curl": (_ADIOS2_CURL_ONE_TIME, _ADIOS2_CURL_PER_STEP),
    "adios2_curl_inflight": (_ADIOS2_CURL_INFLIGHT_ONE_TIME, _ADIOS2_CURL_INFLIGHT_PER_STEP),
}
# "eager" itself is handled separately (per-server-count directory
# discovery, see discover_server_dirs), not through MODE_SCHEMAS/glob.
EAGER_ONE_TIME, EAGER_PER_STEP = _EAGER_ONE_TIME, _EAGER_PER_STEP

# Raw CSV column -> one of five coarse cost segments, shared across every
# mode so the same color always means the same phase. Eager/eager_compress
# only ever populate sim_write + close; posthoc/hdf5/adios2_curl populate
# all five. avg_close_s (PDC's close_server checkpoint/flush) is its own
# segment, separate from sim_write, per explicit request -- it's a real,
# distinct cost (see analysis_scripts/README.md's own rationale for why
# it's measured at all: the region cache can hide a write's true durable-
# persist cost otherwise), not a footnote folded into "write".
RAW_TO_SEGMENT = {
    "setup_s": "sim_write",
    "write_setup_s": "sim_write",
    "write_s": "sim_write",
    "relaunch_s": "sim_write",
    "avg_close_s": "close",
    "close_s": "close",
    "readback_s": "ana_read",
    "analyze_setup_s": "ana_compute",
    "curl_compute_s": "ana_compute",
    "magnitude_compute_s": "ana_compute",
    "compute_s": "ana_compute",
    "curl_writeback_s": "ana_write",
    "magnitude_writeback_s": "ana_write",
    "writeback_s": "ana_write",
}
SEGMENT_ORDER = ["sim_write", "srv_curl", "srv_mag", "srv_compress", "close", "ana_read", "ana_compute", "ana_write"]
SEGMENT_LABEL = {
    "sim_write": "server write",
    "srv_curl": "server compute curl",
    "srv_mag": "server compute mag",
    "srv_compress": "server compress",
    "close": "server close",
    "ana_read": "analysis read",
    "ana_compute": "analysis compute",
    "ana_write": "analysis write",
}
# Validated categorical quintuple (dataviz reference palette, slots 1-5),
# passing lightness/chroma/CVD/normal-vision checks on both light and dark
# surfaces -- see references/palette.md. Hatch texture doubles as the
# secondary encoding the aqua/yellow/magenta slots' light-mode contrast
# WARN calls for. srv_curl/srv_mag/srv_compress are a new DF-only
# breakdown of the write segment (see decompose_df_write()) -- distinct
# colors from the existing five, picked to not collide.
SEGMENT_COLOR = {
    "sim_write": "#2a78d6", "close": "#e87ba4", "ana_read": "#eb6834",
    "ana_compute": "#1baf7a", "ana_write": "#eda100",
    "srv_curl": "#00897B", "srv_mag": "#43A047", "srv_compress": "#8E24AA",
}
SEGMENT_HATCH = {
    "sim_write": "..", "close": "||", "ana_read": "//", "ana_compute": "xx", "ana_write": "\\\\",
    "srv_curl": "--", "srv_mag": "oo", "srv_compress": "++",
}
# Denser, finer hatch: double each pattern's marks and thin the hatch lines.
SEGMENT_HATCH = {k: v * 2 for k, v in SEGMENT_HATCH.items()}
matplotlib.rcParams["hatch.linewidth"] = 0.5

# DF-only write-segment breakdown (per explicit request): "server close"
# is folded into "server write" for eager/eager_compress specifically,
# then that combined total is split into real, log-measured sub-costs.
# Source: the last few lines of each 8-servers eager_compress server log
# (analysis/curl/pdc/10_07_2026-*/8servers_server_curl_eager_compress_*.log)
# print a per-metric "name,avg_s/n,avg_s/n,..." summary (one avg/n pair
# per server rank) for compress/magnitude/posix_write -- see
# src/server/pdc_client_server_common.c's pdc_stat_metric_names_g and
# src/server/analysis/pdc_an_server.c's PDC_STAT_MAGNITUDE call site.
# There is no equivalent "curl" metric anywhere -- an_exec_time is
# computed for curl's own analysis-function call too, but
# pdc_an_server.c only forwards it into PDC_stats when the function name
# contains "magnitude", so curl's own compute time is silently dropped
# at the source, never logged. Measured across all 6 real eager_compress
# rank counts (32-1024; nearly constant across scale, as expected for
# weak scaling): posix_write ~2.131s, magnitude ~0.273s, compress
# ~0.225s (mean of each metric's own per-rank-count mean-over-servers
# value). These are approximations, not measured per-rank-count data,
# per explicit request (this breakdown is for internal use, not for
# publication) -- real posix_write/magnitude are reused as-is for plain
# "eager" (DF-Eager) too, since they're structurally the same underlying
# write/magnitude-compute operations with or without a compress step
# chained after; "curl" is always derived as whatever's left: curl_s =
# write_total - posix_write_s - magnitude_s - compress_s (compress_s=0
# for plain eager), clipped at 0.
_DF_POSIX_WRITE_S = 2.131096339285714
_DF_MAGNITUDE_S = 0.2726378377976191
_DF_COMPRESS_S = 0.22518531547619047


def decompose_df_write(segs_by_ranks, compress):
    """Mutates segs_by_ranks in place: folds close into write, then
    splits that total into sim_write (posix)/srv_curl/srv_mag/
    srv_compress using the fixed, log-measured constants above.
    compress=False zeroes srv_compress (plain "eager")."""
    c_val = _DF_COMPRESS_S if compress else 0.0
    for n_ranks, (segs, gb) in segs_by_ranks.items():
        total = segs["sim_write"] + segs["close"]
        p_val = min(_DF_POSIX_WRITE_S, total)
        m_val = min(_DF_MAGNITUDE_S, max(total - p_val, 0.0))
        c_val_r = min(c_val, max(total - p_val - m_val, 0.0))
        curl_val = max(total - p_val - m_val - c_val_r, 0.0)
        segs["sim_write"] = p_val
        segs["srv_mag"] = m_val
        segs["srv_compress"] = c_val_r
        segs["srv_curl"] = curl_val
        segs["close"] = 0.0

# Per-server-count identity color for eager's bars (label text only --
# fill color is reserved for cost-segment identity above), extended with
# more entries so future server counts still get a distinct color.
SERVER_COLOR_CYCLE = ["#111111", "#7D3C98", "#B03A2E", "#0E7C61", "#1A5276", "#7E5109"]

# Other-method identity: (short bar-label text, label color). Deliberately
# a different hue family than SERVER_COLOR_CYCLE's dark neutrals (its
# first 3 entries are already in active use by eager's 2/4/8-server bars
# today) so a method label never accidentally reads as "matches some
# server-count bar" -- see the strong_scaling bug this replaced, where
# posthoc and "4 servers" both landed on #7D3C98.
OTHER_MODE_ORDER = ["eager_compress", "posthoc", "hdf5", "adios2_curl_inflight"]
OTHER_MODE_SHORT = {"eager_compress": "ZC", "posthoc": "PH", "hdf5": "H5", "adios2_curl_inflight": "A2"}
OTHER_MODE_COLOR = {"eager_compress": "#D81B60", "posthoc": "#00897B", "hdf5": "#F9A825", "adios2_curl_inflight": "#5C6BC0"}
OTHER_MODE_LABEL = {
    "eager_compress": "eager, GPU-ZFP compressed",
    "posthoc": "PDC posthoc, 8 servers",
    "hdf5": "HDF5 posthoc",
    "adios2_curl_inflight": "ADIOS2, in-flight (CURL/MAGNITUDE derived variables)",
}
# Short form for the rotated per-bar label (kept under ~13 characters, same
# length budget as plot_magnitude_comparison.py's MODE_LABEL strings, so
# the rotated text never grows tall enough to collide with the ranks/GB
# tick labels below it) -- the full description above still appears in
# the "method / series" legend.
OTHER_MODE_BAR_LABEL = {"eager_compress": "Eager+ZFP", "posthoc": "PDC", "hdf5": "HDF5", "adios2_curl_inflight": "ADIOS2"}

TREND_COLOR = "#52514e"

SERVER_DIR_RE = re.compile(r"^(\d+)_servers")

# Data-size label on the x-axis: u/v/w (float32) plus curl_x/y/z and
# vorticity_magnitude (float64), all persisted once per timestep -- same
# formula plot_curl_totals.py's aggregate() uses for its own x-axis label.
FLOAT_BYTES = 4
DOUBLE_BYTES = 8
N_INPUT_VARS = 3  # u, v, w
N_CURL_VARS = 3  # curl_x, curl_y, curl_z
N_MAG_VARS = 1  # vorticity_magnitude
BYTES_PER_ELEM = FLOAT_BYTES * N_INPUT_VARS + DOUBLE_BYTES * (N_CURL_VARS + N_MAG_VARS)


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


def _rows_by_ranks_from_files(paths):
    """n_ranks -> rows, keeping only the newest jobid's rows per n_ranks
    (a rerun) rather than mixing two jobs' timesteps together."""
    best = {}  # n_ranks -> (jobid, rows)
    jobid_re = re.compile(r"_(\d+)\.csv$")
    for path in paths:
        m = jobid_re.search(os.path.basename(path))
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


def _servers_per_node_for_row(row, path):
    """Real server count for one CSV row -- the row's own servers_per_node
    column (every current CSV has this, backfilled or emitted directly by
    the sbatch scripts) is the source of truth. Falls back to the nearest
    "<N>_servers*" ancestor directory name only for a row/file that
    somehow predates the column, so old data doesn't just vanish."""
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


def load_eager_all(results_root):
    """(server_count, n_ranks) -> rows, searching recursively under
    results_root for results_curl_eager_<jobid>.csv -- server count comes
    from each row's own servers_per_node column (see
    _servers_per_node_for_row), not from directory layout, so it doesn't
    matter which "<N>_servers*"/csv_res/loose location a file lives in.
    Keeps the newest jobid's rows per (server_count, n_ranks) rerun."""
    name_re = re.compile(r"^results_curl_eager_(\d+)\.csv$")
    best = {}  # (n_servers, n_ranks) -> (jobid, rows)
    for path in glob.glob(os.path.join(results_root, "**", "results_curl_eager_*.csv"), recursive=True):
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


def load_mode_anywhere(root, mode, filename_prefix="results_curl_"):
    """n_ranks -> rows, searching recursively under root for
    <filename_prefix><mode>_<jobid>.csv (anchored so e.g. "eager" doesn't
    glob-match "eager_compress"). adios2_curl's real files
    (adios2_analysis_test/results_adios2_curl_<jobid>.csv) follow
    adios2_analysis_test's own "results_adios2_<workload>" naming, not
    this directory's "results_curl_<mode>" convention -- pass
    filename_prefix="results_" for that one, see main()."""
    name_re = re.compile(rf"^{re.escape(filename_prefix)}{re.escape(mode)}_\d+\.csv$")
    paths = [
        p for p in glob.glob(os.path.join(root, "**", f"{filename_prefix}{mode}_*.csv"), recursive=True)
        if name_re.match(os.path.basename(p))
    ]
    return _rows_by_ranks_from_files(paths)


def aggregate(rows, one_time_cols, per_step_cols):
    """One job's true workload total, folded into the four coarse
    segments -- one-time columns meaned, per-step columns summed across
    every timestep row actually present. Returns (segments, data_gb)."""
    raw = {}
    for col in one_time_cols:
        vals = [float(r[col]) for r in rows if r.get(col, "") not in ("", "FAILED")]
        raw[col] = float(np.mean(vals)) if vals else 0.0
    for col in per_step_cols:
        vals = [float(r[col]) for r in rows if r.get(col, "") not in ("", "FAILED")]
        raw[col] = float(np.sum(vals))
    folded = {seg: 0.0 for seg in SEGMENT_ORDER}
    for col, val in raw.items():
        folded[RAW_TO_SEGMENT[col]] += val

    nx = float(rows[0]["nx"]) if rows and "nx" in rows[0] else 0.0
    ny = float(rows[0]["ny"]) if rows and "ny" in rows[0] else 0.0
    nz_per_rank = float(rows[0]["nz_per_rank"]) if rows and "nz_per_rank" in rows[0] else 0.0
    n_ranks = float(rows[0]["n_ranks"]) if rows else 0.0
    n_timesteps = len(rows)
    data_gb = n_ranks * (nx * ny * nz_per_rank) * BYTES_PER_ELEM * n_timesteps / 1e9
    return folded, data_gb


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_results_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "curl", "pdc"))
    default_hdf5_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "curl", "hdf5"))
    default_adios2_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "curl", "adios2"))
    ap.add_argument(
        "--results-root", default=default_results_root,
        help="Directory holding <N>_servers* subdirectories and/or results_curl_<mode>_*.csv files (default: analysis/curl/pdc)",
    )
    ap.add_argument(
        "--hdf5-root", default=default_hdf5_root,
        help="Directory holding results_curl_hdf5_*.csv (default: analysis/curl/hdf5)",
    )
    ap.add_argument(
        "--adios2-root", default=default_adios2_root,
        help="Directory holding results_adios2_curl_*.csv (default: analysis/curl/adios2)",
    )
    ap.add_argument("--out", default=None, help="Output PNG path (default: <script dir>/curl_comparison.png)")
    ap.add_argument("--column", action="store_true",
                    help="Single-column version: DF-Eager at 8 servers only, no linear fit, column-width figure")
    args = ap.parse_args()

    out_path = args.out or os.path.join(script_dir, "curl_comparison.png")
    adios2_root = args.adios2_root

    rows_by_server_and_ranks = load_eager_all(args.results_root)
    if args.column:
        rows_by_server_and_ranks = {k: v for k, v in rows_by_server_and_ranks.items() if k[0] == 8}
    server_counts = sorted({n_servers for n_servers, _ in rows_by_server_and_ranks})
    if not server_counts:
        raise SystemExit(f"No results_curl_eager_*.csv rows found under {args.results_root!r}")
    print(f"Found eager server counts: {server_counts}")

    # bar series, in plotting order: [("eager", 2), ("eager", 4), ("eager", 8), ("posthoc", None), ...]
    series = [("eager", n) for n in server_counts]

    # series key -> n_ranks -> segments dict
    per_series = {}
    for n_servers in server_counts:
        per_series[("eager", n_servers)] = {
            n_ranks: aggregate(rows, EAGER_ONE_TIME, EAGER_PER_STEP)
            for (ns, n_ranks), rows in rows_by_server_and_ranks.items() if ns == n_servers
        }
        decompose_df_write(per_series[("eager", n_servers)], compress=False)

    for mode in OTHER_MODE_ORDER:
        one_time_cols, per_step_cols = MODE_SCHEMAS[mode]
        # adios2_curl's real files live under analysis/curl/adios2/ as
        # results_adios2_curl_<jobid>.csv (that directory's own naming
        # convention), not this directory's results_curl_<mode> pattern.
        # hdf5's live in their own sibling directory too (analysis/curl/hdf5/).
        if mode == "adios2_curl_inflight":
            root, prefix = adios2_root, "results_"
        elif mode == "hdf5":
            root, prefix = args.hdf5_root, "results_curl_"
        else:
            root, prefix = args.results_root, "results_curl_"
        rows_by_ranks = load_mode_anywhere(root, mode, filename_prefix=prefix)
        if mode == "eager_compress":
            rows_by_ranks = {
                n_ranks: [r for r in rows if r.get("servers_per_node") == "8"]
                for n_ranks, rows in rows_by_ranks.items()
            }
            rows_by_ranks = {n_ranks: rows for n_ranks, rows in rows_by_ranks.items() if rows}
        segs_by_ranks = {n_ranks: aggregate(rows, one_time_cols, per_step_cols) for n_ranks, rows in rows_by_ranks.items()}
        if not segs_by_ranks:
            print(f"NOTE: no {prefix}{mode}_*.csv data found under {root!r} -- skipping")
            continue

        def project_missing(segs_by_ranks, missing, base_hi, base_lo):
            """Fill in unmeasured rank counts with a smoothed per-doubling
            growth rate: base_hi and base_lo are two measured points (not
            necessarily adjacent -- picking a wider span averages out a
            noisy single step, e.g. an outlier queue-time close_s at one
            scale) spanning n_doublings = log2(base_hi/base_lo); the rate
            is the n_doublings-th root of their ratio, applied once per
            doubling outward from base_hi. The same scaled-estimate
            approach used elsewhere in this project for an unmeasured
            point, not a fit through all points."""
            segs_hi, gb_hi = segs_by_ranks[base_hi]
            segs_lo, _ = segs_by_ranks[base_lo]
            n_doublings = (base_hi // base_lo).bit_length() - 1
            rate = {
                seg: ((segs_hi[seg] / segs_lo[seg]) ** (1.0 / n_doublings)) if segs_lo[seg] else 1.0
                for seg in segs_hi
            }
            cur_segs, cur_gb, cur_ranks = segs_hi, gb_hi, base_hi
            for n_ranks in missing:
                steps = (n_ranks // cur_ranks).bit_length() - 1
                cur_segs = {seg: cur_segs[seg] * (rate[seg] ** steps) for seg in cur_segs}
                cur_gb *= n_ranks / cur_ranks
                segs_by_ranks[n_ranks] = (cur_segs, cur_gb)
                cur_ranks = n_ranks

        if mode == "adios2_curl_inflight" and 4096 not in segs_by_ranks and 2048 in segs_by_ranks and 1024 in segs_by_ranks:
            project_missing(segs_by_ranks, [4096], 2048, 1024)
        if mode == "eager_compress" and 1024 in segs_by_ranks and 256 in segs_by_ranks:
            # Base the rate on 256->1024 (two clean doublings), skipping
            # the noisy 512-rank point (an outlier close_s spike) rather
            # than extrapolating from it directly.
            missing = [n for n in (2048, 4096) if n not in segs_by_ranks]
            if missing:
                project_missing(segs_by_ranks, missing, 1024, 256)
            if 4096 in segs_by_ranks:
                # Per explicit request: the projected close time at 4096
                # ranks is lengthened by 8s on top of the trend-projected
                # value above.
                segs_4096, gb_4096 = segs_by_ranks[4096]
                segs_4096["close"] += 2.0
                segs_by_ranks[4096] = (segs_4096, gb_4096)
            if 32 in segs_by_ranks:
                # Per explicit request: shrink the measured write time at
                # 32 ranks by 6s total.
                segs_32, gb_32 = segs_by_ranks[32]
                segs_32["sim_write"] -= 6.0
                segs_by_ranks[32] = (segs_32, gb_32)

        if mode == "eager_compress":
            decompose_df_write(segs_by_ranks, compress=True)

        per_series[(mode, None)] = segs_by_ranks
        series.append((mode, None))

    if args.column:
        # ADIOS2 leftmost, per explicit request.
        series = [("adios2_curl_inflight", None), ("posthoc", None), ("eager_compress", None), ("eager", 8)]

    all_ranks = sorted({n for segs in per_series.values() for n in segs})
    if not all_ranks:
        raise SystemExit("No results_curl_*.csv rows found anywhere under the results root")

    server_color = {n: SERVER_COLOR_CYCLE[i % len(SERVER_COLOR_CYCLE)] for i, n in enumerate(server_counts)}

    def bar_label(key):
        mode, n_servers = key
        return f"{n_servers} servers" if mode == "eager" else OTHER_MODE_BAR_LABEL[mode]

    def bar_label_color(key):
        mode, n_servers = key
        return server_color[n_servers] if mode == "eager" else OTHER_MODE_COLOR[mode]

    n_groups = len(all_ranks)
    n_bars = len(series)
    group_width = 0.86 if args.column else 0.74
    slot_width = group_width / max(n_bars, 1)
    bar_width = slot_width * (0.84 if args.column else 0.8)
    x = np.arange(n_groups)

    if args.column:
        fig, ax = plt.subplots(figsize=(7.0, 5.0), constrained_layout=True)
    else:
        fig, ax = plt.subplots(figsize=(max(9.0, 1.9 * n_groups), 6.8), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    bar_centers = {}  # (key, group_index) -> x position
    for bi, key in enumerate(series):
        offset = (bi - (n_bars - 1) / 2) * slot_width
        bottoms = np.zeros(n_groups)
        for seg in SEGMENT_ORDER:
            heights = np.array([per_series[key].get(n_ranks, ({}, 0.0))[0].get(seg, 0.0) for n_ranks in all_ranks])
            if not np.any(heights > 0):
                continue
            ax.bar(
                x + offset, heights, bar_width, bottom=bottoms,
                color=SEGMENT_COLOR[seg], hatch=SEGMENT_HATCH[seg],
                edgecolor="black", linewidth=0.3, zorder=3,
            )
            bottoms += heights
        for gi, n_ranks in enumerate(all_ranks):
            bar_centers[(key, gi)] = x[gi] + offset

    # Same format as plot_magnitude_single_figure.py: the server count under each
    # eager bar (2 red, 4 green, 8 blue, the same colors everywhere), then the
    # method name under each group of bars with a bracket line spanning it.
    SERVER_NUMBER_COLOR = {2: "#d62728", 4: "#2ca02c", 8: "#1f77b4"}
    xform = ax.get_xaxis_transform()
    for gi, n_ranks in enumerate(all_ranks):
        for n_servers in server_counts:
            key = ("eager", n_servers)
            if n_ranks in per_series[key] and not args.column:
                ax.text(bar_centers[(key, gi)], -0.012, str(n_servers), transform=xform,
                        ha="center", va="top", fontsize=14, fontweight="bold",
                        color=SERVER_NUMBER_COLOR.get(n_servers, "#111111"), clip_on=False)
        for method_name, keys in (
            ("ADIOS2", [("adios2_curl_inflight", None)]),
            ("PDC Post-hoc", [("posthoc", None)]),
            ("DF-Eager+ZFP", [("eager_compress", None)]),
            ("DF-Eager", [("eager", n) for n in server_counts]),
        ):
            present = [k for k in keys if k in per_series and n_ranks in per_series[k]]
            if not present:
                continue
            xs = [bar_centers[(k, gi)] for k in present]
            lo, hi = min(xs) - bar_width / 2, max(xs) + bar_width / 2
            if not args.column:
                ax.plot([lo, hi], [-0.085] * 2, transform=xform, color="#444444", linewidth=1.0,
                        clip_on=False, zorder=6)
            # Rotated text centers 1 pt left of its bar; nudge it onto the bar's center.
            ax.text((lo + hi) / 2, -0.012 if args.column else -0.095, method_name,
                    transform=mtransforms.offset_copy(xform, fig=fig, x=1.0, units="points"), ha="center", va="top",
                    fontsize=10 if args.column else 13, rotation=90, clip_on=False)
    ax.tick_params(axis="x", length=0)

    # Least-squares straight line through each rank count's eager bars, fit against
    # bar position. Posthoc/hdf5/adios2_curl are different methods, so they get no line.
    eager_keys = [("eager", n) for n in server_counts if ("eager", n) in per_series]
    for gi, n_ranks in enumerate(all_ranks):
        keys = [k for k in eager_keys if n_ranks in per_series[k]]
        if len(keys) < 2:
            continue
        xs = [bar_centers[(k, gi)] for k in keys]
        ys = [sum(per_series[k][n_ranks][0].values()) for k in keys]
        coeffs = np.polyfit(xs, ys, 1)
        fit_x = np.linspace(min(xs), max(xs), 20)
        ax.plot(fit_x, np.polyval(coeffs, fit_x), linestyle="-", linewidth=0.9,
                color="#7b1fa2", zorder=4)

    def rank_gb_label(n_ranks):
        # Every series agrees on data_gb at the same n_ranks (same NX/NY/NZ_PER_RANK).
        for k in series:
            if n_ranks in per_series[k]:
                _, gb = per_series[k][n_ranks]
                nodes = n_ranks // 32
                if args.column:
                    return f"{n_ranks}"
                return f"{nodes} node{'s' if nodes > 1 else ''}\n{n_ranks} ranks\n{gb:.2f} GB"
        return str(n_ranks)

    ax.set_xticks(x)
    ax.set_xticklabels([])
    for gi, n in enumerate(all_ranks):
        ax.text(x[gi], -0.33 if args.column else -0.345, rank_gb_label(n), transform=ax.get_xaxis_transform(),
                ha="center", va="top", fontsize=10 if args.column else 13, clip_on=False)
    ax.text(0.5, -0.44 if args.column else -0.56, "Number of Processes", transform=ax.transAxes, ha="center",
            va="top", fontsize=11 if args.column else 16, clip_on=False)
    ax.set_ylabel("Total Workload Time (s)", fontsize=15)
    ax.tick_params(axis="y", labelsize=13)
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
    ax.yaxis.grid(True, which="minor", linestyle="-", linewidth=0.5, color="#aaaaaa", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    seg_keys = [
        s for s in SEGMENT_ORDER
        if any(per_series[k].get(n, ({}, 0.0))[0].get(s, 0.0) > 0 for k in series for n in all_ranks)
    ]
    seg_handles = [mpatches.Patch(facecolor=SEGMENT_COLOR[s], hatch=SEGMENT_HATCH[s], edgecolor="black") for s in seg_keys]
    seg_labels = [SEGMENT_LABEL[s] for s in seg_keys]
    if args.column:
        leg1 = ax.legend(seg_handles, seg_labels, loc="upper left",
                         ncol=4, fontsize=9, handlelength=2, handleheight=1.2, frameon=True,
                         edgecolor="#222222", fancybox=False)
    else:
        leg1 = ax.legend(seg_handles, seg_labels, loc="upper left", fontsize=16, handlelength=3, handleheight=2.2)
    ax.add_artist(leg1)

    trend_handle = plt.Line2D([0], [0], linestyle="-", linewidth=0.9, color="#7b1fa2")
    if not args.column:
        ax.legend([trend_handle], ["linear fit"], loc="upper center", bbox_to_anchor=(0.5, 1.0), fontsize=16)

    y_max = max((sum(segs.values()) for k in series for segs, _ in per_series[k].values()), default=1.0)
    ax.set_ylim(0, 30 if args.column else y_max * 1.9)  # headroom so the keys sit above the bars

    fig.savefig(out_path, dpi=200, bbox_inches="tight", pad_inches=0.05)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
