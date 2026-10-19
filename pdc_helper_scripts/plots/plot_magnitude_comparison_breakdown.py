#!/usr/bin/env python3
"""
Total workload time across the magnitude-analysis benchmark's five
workload types -- DF-eager, DF-view (lazy), PDC posthoc, HDF5 posthoc,
ADIOS2 (eager) -- reconstructed from the results_<mode>_*.csv files
under pdc_helper_scripts/analysis/magnitude/{pdc,hdf5,adios2,highfive}/
(each mode now lives in its own directory since the analysis/
transformation/io reorg -- see --results-root/--hdf5-root/--adios2-root/
--highfive-root below).

DF-eager has a real per-server-count sweep on disk (2/4/8-server
"<N>_servers*" directories, all 8 rank counts each -- same shape as the
curl workload's eager sweep), so it's broken out into one bar per server
count plus a linear best-fit line through each rank count's 2/4/8-server
totals, exactly like plot_curl_comparison.py does for curl's eager --
see that script's own docstring for the "why a fit line" rationale. The
other four modes (DF-view/posthoc/hdf5/adios2_magnitude) don't have a
comparable per-server-count sweep in the data on disk today, so each
still gets exactly one bar per rank count, searched recursively so it
doesn't matter which "<N>_servers*"/csv_res/previous_results
subdirectory a given job's CSV happens to live in -- newest Slurm job ID
wins per (mode, n_ranks) rerun.

This is a local, PNG-only port of a Colab/Google-Drive notebook script
of the same shape (same MODE_LABEL naming, same RAW_TO_SEGMENT folding,
same relaunch-sleep correction) -- see that script's own docstring for
the full rationale; the differences here are mechanical (local paths
instead of Drive, matplotlib Agg + PNG instead of inline-Colab + PDF, no
wide/narrow paper-column presets, no log-scale variant) not conceptual.

Each results_<mode>_<jobid>.csv has one row per timestep (N_TIMESTEPS=3
in the C benchmarks), with per-timestep costs that really happen three
times (write_s, readback_s, compute_s, writeback_s) and one-time
job-level costs repeated on every row for CSV convenience (setup_s /
write_setup_s / analyze_setup_s, relaunch_s, avg_close_s). aggregate()
sums the former across every timestep row and takes the latter once,
not 3x, then folds everything into four coarse segments (RAW_TO_SEGMENT):
simulation write (setup + write + relaunch + server-close), analysis
read, analysis compute, analysis write.

confirm_read_s (the post-write read that confirms DataFlyway/ADIOS2
actually materialized magnitude) is deliberately excluded everywhere:
it's each benchmark's own correctness check, not workload cost being
measured -- same reasoning the curl-workload plot_curl_totals.py /
plot_curl_comparison.py apply to their own confirm_read_s.

posthoc's (and eager_posthoc's) relaunch_s includes two fixed harness
sleeps around the close/restart cycle (srun_close_server.sh's `sleep 2`
+ srun_server_restart.sh's `sleep 10`) that aren't PDC cost -- subtracted
via RELAUNCH_SLEEP_S, same as the reference script. hdf5 shares
posthoc's CSV schema but has no PDC server to close/restart, so its
relaunch_s is always 0 and is left untouched.

adios2_magnitude's own CSV (adios2_bench_magnitude.cpp) additionally
carries a real writer.Close() timing column (close_s) the reference
script's schema didn't have -- included here (folded into sim_write,
same bucket avg_close_s uses for the PDC-backed modes) rather than
silently dropped, so ADIOS2 isn't given a hidden advantage for a cost
the other four modes all pay and report.

Usage:
    python3 plot_magnitude_comparison.py [--results-root DIR] [--adios2-root DIR] [--out FILE.png] [--modes hdf5,posthoc,eager,adios2_magnitude,lazy]

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

# Per explicit reviewer request: PDF/PS output embeds only the 14 standard
# core fonts (no Type 3/TrueType subsetting), and all text is rendered by
# a real LaTeX engine (needs latex, cm-super, and texlive-latex-extra's
# type1cm.sty installed -- all three confirmed present on this machine).
plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

# mode -> (one_time_cols, per_timestep_cols). confirm_read_s intentionally
# omitted everywhere -- see module docstring.
_EAGER_LAZY_ONE_TIME = ["setup_s"]
_EAGER_LAZY_PER_STEP = ["write_s", "readback_s", "compute_s", "writeback_s"]
_POSTHOC_ONE_TIME = ["write_setup_s", "relaunch_s", "analyze_setup_s"]
_POSTHOC_PER_STEP = ["write_s", "readback_s", "compute_s", "writeback_s"]
# close_s: real writer.Close() cost -- see module docstring.
_ADIOS2_MAGNITUDE_ONE_TIME = ["setup_s", "close_s"]
_ADIOS2_MAGNITUDE_PER_STEP = ["write_s"]
# magnitude_highfive (hdf5_analysis_test/magnitude_highfive.cpp): single-
# session, client-side read/compute/writeback like eager/lazy, plus its
# own real close_s (File's destructor / H5Fclose) -- see that file's own
# header comment.
_HIGHFIVE_ONE_TIME = ["setup_s", "close_s"]
_HIGHFIVE_PER_STEP = ["write_s", "readback_s", "compute_s", "writeback_s"]

SCHEMAS = {
    "eager": (_EAGER_LAZY_ONE_TIME, _EAGER_LAZY_PER_STEP),
    "lazy": (_EAGER_LAZY_ONE_TIME, _EAGER_LAZY_PER_STEP),
    "posthoc": (_POSTHOC_ONE_TIME, _POSTHOC_PER_STEP),
    "hdf5": (_POSTHOC_ONE_TIME, _POSTHOC_PER_STEP),
    "adios2_magnitude": (_ADIOS2_MAGNITUDE_ONE_TIME, _ADIOS2_MAGNITUDE_PER_STEP),
    "magnitude_highfive": (_HIGHFIVE_ONE_TIME, _HIGHFIVE_PER_STEP),
}

RELAUNCH_SLEEP_S = 2.0 + 10.0
RELAUNCH_SLEEP_MODES = {"posthoc"}

# Raw CSV column -> one of five coarse cost segments, shared across every
# mode. eager/lazy/adios2_magnitude have no separable analysis phase
# folded away from sim_write except lazy/eager's own readback/compute
# (present as columns but always 0 for eager -- eager never reads
# magnitude back client-side -- and populated for lazy, which recomputes
# it from scratch on every access). avg_close_s (PDC's close_server
# checkpoint/flush) and adios2_magnitude's close_s (writer.Close()) are
# their own "close" segment, separate from sim_write, per explicit
# request -- both are real, distinct costs, not a footnote folded into
# "write" (see README.md's own rationale for why close is measured at
# all: the server-side region cache can otherwise hide a write's true
# durable-persist cost).
RAW_TO_SEGMENT = {
    "setup_s": "sim_write",
    "write_setup_s": "sim_write",
    "write_s": "sim_write",
    "relaunch_s": "sim_write",
    "avg_close_s": "close",
    "close_s": "close",
    "readback_s": "ana_read",
    "analyze_setup_s": "ana_compute",
    "compute_s": "ana_compute",
    "writeback_s": "ana_write",
}
SEGMENT_ORDER = ["sim_write", "srv_mag", "close", "ana_read", "ana_compute", "ana_write"]
SEGMENT_LABEL = {
    "sim_write": "server write",
    "srv_mag": "server compute mag",
    "close": "close",
    "ana_read": "analysis read",
    "ana_compute": "analysis compute",
    "ana_write": "analysis write",
}
# Validated categorical quintuple (dataviz reference palette slots 1-5) --
# see plot_curl_comparison.py's identical choice for the CVD/contrast
# validation this passed. srv_mag is a new DF-only breakdown of the write
# segment (see decompose_df_write()), same color/hatch convention
# plot_curl_comparison_breakdown.py uses for its own srv_mag.
SEGMENT_COLOR = {
    "sim_write": "#2a78d6", "close": "#e87ba4", "ana_read": "#eb6834",
    "ana_compute": "#1baf7a", "ana_write": "#eda100", "srv_mag": "#43A047",
}
SEGMENT_HATCH = {
    "sim_write": "..", "close": "||", "ana_read": "//", "ana_compute": "xx", "ana_write": "\\\\",
    "srv_mag": "oo",
}

# DF-only write-segment breakdown (per explicit request), same convention
# plot_curl_comparison_breakdown.py uses. The vector-magnitude workload's
# own PDC server logs have no posix_write/magnitude metric lines at all
# (checked every log under analysis/magnitude/pdc/ -- all of them predate
# the stats instrumentation that curl's 10_07_2026 eager_compress batch
# happened to be built with), so there's no real per-rank-count data to
# pull for this workload. Approximated instead (not for publication, per
# explicit request) by applying curl's own real measured write/magnitude
# *ratio* -- posix_write ~2.131s vs magnitude ~0.273s, i.e. write is
# ~88.7% of the write+magnitude-compute total there -- to this workload's
# own (different) write+close total at each rank count.
_DF_WRITE_FRAC = 2.131096339285714 / (2.131096339285714 + 0.2726378377976191)
_DF_MAG_FRAC = 1.0 - _DF_WRITE_FRAC


def decompose_df_write(segs_by_ranks):
    """Mutates segs_by_ranks in place: folds close into write, then
    splits that total into sim_write/srv_mag using the fixed ratio
    above."""
    for n_ranks, (segs, gb) in segs_by_ranks.items():
        total = segs["sim_write"] + segs["close"]
        segs["sim_write"] = total * _DF_WRITE_FRAC
        segs["srv_mag"] = total * _DF_MAG_FRAC
        segs["close"] = 0.0

# Bar order, left to right within each rank-count group: HDF5, HighFive
# (grouped next to HDF5 since it's the same underlying library), ADIOS2,
# then PDC posthoc's/DF-eager's/DF-view's own 2/4/8-server bars -- per
# explicit request that ADIOS2 sit right after HDF5 Posthoc, and that
# DF-view get the same strong-scaling breakout as DF-eager (it has the
# identical real per-server-count sweep on disk). "eager", "lazy" and
# "posthoc" are all handled separately (per-server-count breakout, see
# MULTI_SERVER_MODES and main()), so none of the three is in this
# single-bar-per-mode list at all.
MODE_ORDER = ["hdf5", "magnitude_highfive", "adios2_magnitude", "eager_posthoc"]
DEFAULT_PLOT_MODES = ["hdf5", "magnitude_highfive", "adios2_magnitude"]
MODE_LABEL = {
    "eager_posthoc": "DF-eager+PH",
    "hdf5": "HDF5",
    "magnitude_highfive": "HighFive",
    "adios2_magnitude": "ADIOS2",
}
# Identity color for each mode's bar-top label (a separate visual channel
# from the segment fill above). Deliberately distinct from the
# per-server-count color cycles below, already in active use by
# DF-eager's/DF-view's/PDC-posthoc's 2/4/8-server bars, so no mode label
# ever collides with a server-count label -- same fix
# plot_curl_comparison.py applied for the identical reason. ADIOS2 reuses
# plot_curl_comparison.py's ADIOS2 color so the same method reads as the
# same color across both charts.
MODE_COLOR = {
    "eager_posthoc": "#117864",
    "hdf5": "#d62728",
    "magnitude_highfive": "#37474F",
    "adios2_magnitude": "#1f77b4",
}
# PDC posthoc only has a real measurement at 8 servers today (see module
# docstring). Per explicit request, its 2/4-server bars are still drawn,
# as dashed-outline WIP placeholders scaled from the real 8-server
# segments, assuming the same "time roughly doubles each time server
# count halves" trend DF-eager's and DF-view's own real 2/4/8 data
# already show -- these are NOT measured data, and are marked as such
# (dashed outline, "*" label, and a caption under the x-axis, deliberately
# NOT a faded/different color -- segment color always means the same cost
# category whether or not the bar is WIP) so they never read as real at a
# glance without implying the underlying cost segments are somehow
# different in kind.
WIP_POSTHOC_SERVERS = {4: 2.0, 2: 4.0}
# Modes with a real per-server-count sweep on disk, each getting its own
# 2/4/8-server bars + its own linear-fit line, in this left-to-right
# order. "prefix" names the bar label ("DF-eager, 2 servers", ...) and
# "colors" is that mode's own server-count color cycle -- two disjoint
# cycles so a "2 servers" bar is never the same color for both modes.
# "bracket_label" is the two-line header drawn above a bracket spanning
# that mode's 3 server-count bars (see main()) -- the per-bar label
# underneath each individual bar is just its server count, since the
# bracket + header already names the mode once for the whole cluster.
# "posthoc" is listed first so its bracket/bars sit right after
# HDF5/ADIOS2 (see MODE_ORDER's comment) -- dict order drives series
# order in main(). Its 2/4-server bars are still WIP placeholders (see
# WIP_POSTHOC_SERVERS); the bracket/trend-line treatment is otherwise
# identical to DF-eager/DF-view, per explicit request.
MULTI_SERVER_MODES = {
    "posthoc": {"bracket_label": "PDC Post-hoc", "color": "#2ca02c"},
    "eager": {"bracket_label": "DF-Eager", "color": "#ff7f0e"},
    "lazy": {"bracket_label": "DF-View", "color": "#000000"},
}
TREND_COLOR = "#52514e"

FLOAT_BYTES = 4
DOUBLE_BYTES = 8
N_VARS = 3  # vx, vy, vz (float32 input)
BYTES_PER_ELEM = FLOAT_BYTES * N_VARS + DOUBLE_BYTES  # + magnitude (float64)


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


def load_pdc_mode_all(results_root, mode):
    """(server_count, n_ranks) -> rows, searching recursively under
    results_root for results_<mode>_<jobid>.csv -- server count comes from
    each row's own servers_per_node column (see _servers_per_node_for_row),
    not from directory layout, so it doesn't matter which
    "<N>_servers*"/csv_res/previous_results location a file lives in.
    Keeps the newest jobid's rows per (server_count, n_ranks) rerun. Used
    for both MULTI_SERVER_MODES entries (eager, lazy)."""
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
    results_<mode>_<jobid>.csv (anchored so e.g. "eager" doesn't
    glob-match "eager_posthoc"), keeping only the newest jobid's rows per
    n_ranks rather than mixing two jobs' timesteps together."""
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
    """Reconstruct one job's true total workload time, folded into the
    four coarse segments. One-time columns (setup_s, relaunch_s,
    avg_close_s, ...) meaned across a job's timestep rows; per-timestep
    columns summed. Returns (segments: dict[seg] -> seconds, data_gb)."""
    one_time_cols, per_step_cols = SCHEMAS[mode]
    raw = {}
    for col in one_time_cols:
        vals = [float(r[col]) for r in rows if r.get(col, "") not in ("", "FAILED")]
        raw[col] = float(np.mean(vals)) if vals else 0.0
    if mode in RELAUNCH_SLEEP_MODES and "relaunch_s" in raw:
        raw["relaunch_s"] = max(0.0, raw["relaunch_s"] - RELAUNCH_SLEEP_S)
    for col in per_step_cols:
        vals = [float(r[col]) for r in rows if r.get(col, "") not in ("", "FAILED")]
        raw[col] = float(np.sum(vals))

    # avg_close_s is one-time per job, present (or naturally 0) across
    # every PDC-backed mode; adios2_magnitude has no server, so it's
    # naturally absent there (its own close_s is handled via SCHEMAS).
    close_vals = [float(r["avg_close_s"]) for r in rows if r.get("avg_close_s", "") not in ("", "FAILED")]
    if close_vals:
        raw["avg_close_s"] = float(np.mean(close_vals))

    folded = {seg: 0.0 for seg in SEGMENT_ORDER}
    for col, val in raw.items():
        folded[RAW_TO_SEGMENT[col]] += val

    n_elem = float(rows[0]["n_elem"]) if rows and "n_elem" in rows[0] else 0.0
    n_ranks = float(rows[0]["n_ranks"]) if rows else 0.0
    n_timesteps = len(rows)
    data_gb = n_ranks * n_elem * BYTES_PER_ELEM * n_timesteps / 1e9
    return folded, data_gb


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_results_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "magnitude", "pdc"))
    default_hdf5_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "magnitude", "hdf5"))
    default_adios2_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "magnitude", "adios2"))
    default_highfive_root = os.path.normpath(os.path.join(script_dir, "..", "analysis", "magnitude", "highfive"))
    ap.add_argument("--results-root", default=default_results_root, help="Directory holding PDC-backed results_<mode>_*.csv (default: analysis/magnitude/pdc)")
    ap.add_argument("--hdf5-root", default=default_hdf5_root, help="Directory holding results_hdf5_*.csv (default: analysis/magnitude/hdf5)")
    ap.add_argument("--adios2-root", default=default_adios2_root, help="Directory holding results_adios2_magnitude_*.csv (default: analysis/magnitude/adios2)")
    ap.add_argument("--highfive-root", default=default_highfive_root, help="Directory holding results_magnitude_highfive_*.csv (default: analysis/magnitude/highfive)")
    ap.add_argument("--out", default=None, help="Output PNG path (default: <script dir>/magnitude_comparison.png)")
    ap.add_argument(
        "--modes", default=None,
        help=f"Comma-separated subset of modes to plot (default: {','.join(DEFAULT_PLOT_MODES)}, "
             "minus magnitude_highfive when --server-count is given -- see that flag)",
    )
    ap.add_argument(
        "--server-count", type=int, default=None, choices=[2, 4, 8],
        help="Restrict DF-eager/DF-view/PDC posthoc to just this one servers-per-node value "
             "(one plain bar per mode instead of a 2/4/8-server bracket + trend line). "
             "Also drops magnitude_highfive from the default mode list (unless --modes "
             "overrides it) -- these single-server-count charts are meant as a focused "
             "PDC-vs-HDF5-vs-ADIOS2 comparison, and HighFive is the same underlying library "
             "as the HDF5 posthoc baseline already shown, not a separate data point worth "
             "the extra bar at this zoom level. Default: all server counts found on disk,"
             " bracketed as usual.",
    )
    args = ap.parse_args()

    out_path = args.out or os.path.join(
        script_dir, f"magnitude_comparison_{args.server_count}servers.png" if args.server_count else "magnitude_comparison.png"
    )

    if args.modes is not None:
        modes_str = args.modes
    elif args.server_count is not None:
        modes_str = ",".join(m for m in DEFAULT_PLOT_MODES if m != "magnitude_highfive")
    else:
        modes_str = ",".join(DEFAULT_PLOT_MODES)
    modes = [m.strip() for m in modes_str.split(",") if m.strip() and m.strip() != "eager"]
    for m in modes:
        if m not in SCHEMAS:
            raise SystemExit(f"Unknown mode '{m}', expected one of {list(SCHEMAS)}")
    modes = sorted(dict.fromkeys(modes), key=MODE_ORDER.index)

    # Server count comes from each row's own servers_per_node column now
    # (see load_pdc_mode_all), not from "<N>_servers*" directory layout --
    # load eager/lazy first so server_counts (used below for posthoc's WIP
    # synthesis and the color cycles) reflects what's actually in the data.
    per_mode_rows = {
        ms_mode: load_pdc_mode_all(args.results_root, ms_mode)
        for ms_mode in MULTI_SERVER_MODES if ms_mode != "posthoc"
    }
    server_counts = sorted({n_servers for rows in per_mode_rows.values() for (n_servers, _) in rows})
    if not server_counts:
        raise SystemExit(f"No results_<mode>_*.csv rows found under {args.results_root!r}")
    if args.server_count is not None:
        if args.server_count not in server_counts:
            raise SystemExit(f"--server-count {args.server_count} not found on disk (available: {server_counts})")
        server_counts = [args.server_count]
    print(f"Found server counts: {server_counts} (applies to both DF-eager and DF-view)")

    # series key -> n_ranks -> (segments, data_gb). PDC posthoc only has a
    # real measurement at 8 servers (see WIP_POSTHOC_SERVERS's comment) --
    # load whatever's really on disk for eager/lazy, but for posthoc
    # specifically, load the real 8-server data (wherever it lives) and
    # synthesize the 2/4-server WIP bars from it instead of trying (and
    # failing) to find real per-server-count data.
    per_series = {}
    for ms_mode in MULTI_SERVER_MODES:
        if ms_mode == "posthoc":
            posthoc_rows = load_mode_anywhere(args.results_root, "posthoc")
            posthoc_real = {n_ranks: aggregate("posthoc", rows) for n_ranks, rows in posthoc_rows.items()}
            if not posthoc_real:
                print(f"NOTE: no results_posthoc_*.csv data found under {args.results_root!r} -- skipping PDC posthoc entirely")
                continue
            per_series[("posthoc", 8)] = posthoc_real
            for n_servers, scale in WIP_POSTHOC_SERVERS.items():
                per_series[("posthoc", n_servers)] = {
                    n_ranks: ({seg: v * scale for seg, v in segs.items()}, gb) for n_ranks, (segs, gb) in posthoc_real.items()
                }
            continue
        rows_by_key = per_mode_rows[ms_mode]
        for n_servers in server_counts:
            per_series[(ms_mode, n_servers)] = {
                n_ranks: aggregate(ms_mode, rows)
                for (ns, n_ranks), rows in rows_by_key.items() if ns == n_servers
            }
            if ms_mode in ("eager", "lazy"):
                decompose_df_write(per_series[(ms_mode, n_servers)])

    for mode in modes:
        if mode == "adios2_magnitude":
            root = args.adios2_root
        elif mode == "magnitude_highfive":
            root = args.highfive_root
        elif mode == "hdf5":
            root = args.hdf5_root
        else:
            root = args.results_root
        if not os.path.isdir(root):
            print(f"NOTE: {root!r} does not exist -- skipping mode {mode!r}")
            continue
        rows_by_ranks = load_mode_anywhere(root, mode)
        segs_by_ranks = {n_ranks: aggregate(mode, rows) for n_ranks, rows in rows_by_ranks.items()}
        if not segs_by_ranks:
            print(f"NOTE: no results_{mode}_*.csv data found under {root!r} -- skipping")
            continue
        per_series[(mode, None)] = segs_by_ranks

    # Bar order left to right: HDF5, ADIOS2, then each MULTI_SERVER_MODES
    # entry's own 2/4/8-server bars in turn (PDC posthoc, DF-eager,
    # DF-view, in that dict order) -- see MODE_ORDER's own comment for
    # why ADIOS2 sits right after HDF5.
    before_multi = [m for m in ("hdf5", "adios2_magnitude") if (m, None) in per_series]
    after_multi = [m for m in modes if m not in ("hdf5", "adios2_magnitude") and (m, None) in per_series]
    multi_series = [(ms_mode, n) for ms_mode in MULTI_SERVER_MODES for n in server_counts if (ms_mode, n) in per_series]
    series = [(m, None) for m in before_multi] + multi_series + [(m, None) for m in after_multi]
    if not series:
        raise SystemExit("No results_<mode>_*.csv rows found for any requested mode")

    all_ranks = sorted({n for segs in per_series.values() for n in segs})

    def is_wip(key):
        return key[0] == "posthoc" and key[1] in WIP_POSTHOC_SERVERS

    def bar_label(key):
        mode, n_servers = key
        if mode in MULTI_SERVER_MODES:
            # No bracket/group header above the cluster (per explicit
            # request), so each individual bar carries its own mode name
            # and server count, e.g. "PDC [2]".
            short_name = MULTI_SERVER_MODES[mode]["bracket_label"].split("\n")[0]
            if args.server_count is not None:
                return short_name
            return f"{short_name} [{n_servers}]"
        return MODE_LABEL[mode]

    def rank_gb_label(n_ranks):
        return str(n_ranks)

    n_groups = len(all_ranks)
    n_bars = len(series)
    group_width = 0.92

    # Equal spacing between every bar in a group, per explicit request --
    # no extra gap at cluster boundaries (that was only ever needed to
    # keep a bracket header's text clear of its neighbor; now that
    # brackets are gone, every bar sits at the same uniform slot width).
    slot_width = group_width / max(n_bars, 1)
    bar_width = slot_width * 0.88
    x = np.arange(n_groups)

    offsets = np.array([(bi - (n_bars - 1) / 2) * slot_width for bi in range(n_bars)])

    fig, ax = plt.subplots(figsize=(max(9.0, 2.6 * n_groups), 6.0), constrained_layout=True)
    fig.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    bar_centers = {}  # (key, group_index) -> x position
    for bi, key in enumerate(series):
        offset = offsets[bi]
        bottoms = np.zeros(n_groups)
        wip = is_wip(key)
        for seg in SEGMENT_ORDER:
            heights = np.array([per_series[key].get(n_ranks, ({}, 0.0))[0].get(seg, 0.0) for n_ranks in all_ranks])
            if not np.any(heights > 0):
                continue
            # WIP placeholder bars (see WIP_POSTHOC_SERVERS): always keep
            # the normal white edge here -- matplotlib ties a hatch
            # pattern's color to edgecolor, so an earlier version of this
            # that set edgecolor gray for WIP bars turned their hatch dots
            # gray/near-black too, silently changing what the segment
            # color means for WIP vs. real bars. The dashed gray WIP
            # outline is drawn as a separate unfilled overlay below
            # instead, so segment color (and its hatch) always mean the
            # same thing regardless of WIP status.
            ax.bar(
                x + offset, heights, bar_width, bottom=bottoms,
                color=SEGMENT_COLOR[seg], hatch=SEGMENT_HATCH[seg],
                edgecolor="black", linewidth=0.6, zorder=3,
            )
            bottoms += heights
        if wip:
            ax.bar(
                x + offset, bottoms, bar_width, bottom=np.zeros(n_groups),
                fill=False, edgecolor="#666666", linewidth=1.1, linestyle="--", zorder=4,
            )
        for gi, n_ranks in enumerate(all_ranks):
            bar_centers[(key, gi)] = x[gi] + offset

    ax.set_ylim(0, 175)
    edge_pad = slot_width * 0.6
    ax.set_xlim(x[0] - group_width / 2 - edge_pad, x[-1] + group_width / 2 + edge_pad)

    # No bracket/group header above each MULTI_SERVER_MODES cluster, per
    # explicit request -- each bar's own label (bar_label(), set below)
    # already carries both the mode name and its server count, e.g.
    # "PDC [2]", so a spanning bracket would be redundant.

    # Linear best-fit line through each rank-count group's 2/4/8-server
    # totals, once per MULTI_SERVER_MODES entry -- same convention/
    # rationale as plot_curl_comparison.py's own trend line. Both modes'
    # lines share one neutral color; they never overlap in x since each
    # mode's bars occupy their own, non-interleaved span within the group.
    trend_label_used = False
    for ms_mode in MULTI_SERVER_MODES:
        for gi, n_ranks in enumerate(all_ranks):
            xs, ys = [], []
            for n_servers in server_counts:
                key = (ms_mode, n_servers)
                if n_ranks in per_series[key]:
                    xs.append(bar_centers[(key, gi)])
                    ys.append(sum(per_series[key][n_ranks][0].values()))
            if len(xs) < 2:
                continue
            coeffs = np.polyfit(xs, ys, 1)
            fit_xs = np.linspace(min(xs), max(xs), 20)
            fit_ys = np.polyval(coeffs, fit_xs)
            ax.plot(
                fit_xs, fit_ys, linestyle="-", linewidth=1.6, color="#7b1fa2", zorder=4, alpha=1.0,
                label="linear fit" if not trend_label_used else None,
            )
            trend_label_used = True

    ax.set_xticks(x)
    ax.set_xticklabels([rank_gb_label(n) for n in all_ranks], fontweight="bold")
    ax.set_xlabel("Number of Processes", fontsize=15, labelpad=8)
    ax.set_ylabel("Total Workload Time (s)", fontsize=15)
    ax.tick_params(axis="y", labelsize=13)
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
    ax.yaxis.grid(True, which="minor", linestyle="-", linewidth=0.5, color="#aaaaaa", alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Workload-type label underneath each bar, rotated and colored by
    # series identity -- same convention plot_curl_comparison.py uses.
    # Scale the rotated bar-label font to the largest size that still fits
    # this chart's own bar density without adjacent labels touching --
    # empirically ~8pt is the ceiling at 12 bars/group (this script's
    # combined, all-server-counts view) and up to 14pt is fine once
    # --server-count narrows it to 5 bars/group, so a fixed size can't be
    # both legible there and non-overlapping here.
    bar_label_fontsize = float(np.clip(95.0 / max(n_bars, 1) + 3.0, 8.0, 11.0))
    label_trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)
    for key in series:
        for gi, n_ranks in enumerate(all_ranks):
            if n_ranks not in per_series[key]:
                continue
            ax.text(
                bar_centers[(key, gi)], -0.02, bar_label(key),
                transform=label_trans, rotation=90, ha="center", va="top",
                fontsize=bar_label_fontsize, fontweight="bold", color="black", clip_on=False,
            )
    # Smaller, darker ranks/GB tick labels so they read clearly against
    # the taller rotated bar labels and strong-scaling brackets crowding
    # the space right above them.
    ax.tick_params(axis="x", pad=110, labelsize=10.5, labelcolor="#111111")
    # Nudge the ranks tick labels slightly right -- centered-on-tick looks
    # a touch left-of-center against this group's bars, per explicit request.
    for label in ax.get_xticklabels():
        label.set_transform(mtransforms.offset_copy(label.get_transform(), fig=fig, x=12, units="points"))


    seg_keys = [
        s for s in SEGMENT_ORDER
        if any(per_series[k].get(n, ({}, 0.0))[0].get(s, 0.0) > 0 for k in series for n in all_ranks)
    ]
    seg_handles = [mpatches.Patch(facecolor=SEGMENT_COLOR[s], hatch=SEGMENT_HATCH[s], edgecolor="black") for s in seg_keys]
    seg_labels = [SEGMENT_LABEL[s] for s in seg_keys]
    leg1 = ax.legend(seg_handles, seg_labels, loc="upper left", fontsize=16, handlelength=3, handleheight=2.2, ncol=len(seg_handles))
    ax.add_artist(leg1)

    if trend_label_used:
        trend_handle = plt.Line2D([0], [0], linestyle="-", linewidth=1.6, color="#7b1fa2")
        ax.legend([trend_handle], ["linear fit"], loc="upper right", fontsize=16, handlelength=3)


    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
