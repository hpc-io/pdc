#!/usr/bin/env python3
"""
Sync vs async comparison plots for vpic_bdcats scaling-sweep results
(run_sync.sh / run_async.sh's vpic_bdcats_<mode>_<nodes>.csv files, one
per node count -- see tuo_scale_scripts/README.md's CSV schema section).

Reads every vpic_bdcats_<mode>_<nodes>.csv found under the given
directories, using each file's own trailing metadata comment line
(`# n_nodes=...,servers_per_node=...,clients_per_node=...,mode=...,
steps=...,numparticles_per_rank=...,async_sleep_s=...`) rather than
parsing the filename or hardcoding this benchmark's byte-per-particle
constant -- ranks = clients_per_node * n_nodes, and data volume comes
straight from the CSV's own total_data_size_bytes row.

Produces six figures. write and read are ALWAYS separate figures --
nothing here draws a write+read stack or combines the two directions
into one number.
  1/2. Stacked bar (write_stacked_bar.png, read_stacked_bar.png), one
     figure per direction: sync and async bars per node count, EACH
     stacked by PDC operation (object create/open, transfer create,
     transfer start_all_mpi, transfer wait_all, transfer close, object
     close) -- requires the api_call_write/api_call_read CSV rows (see
     below); older CSVs from before that split only have pooled
     `api_call` rows and can't produce these two figures.

     Each operation's stacked segment is `mean_s * count / ranks` from
     that direction's api_call_write/api_call_read rows -- mean call
     duration times calls-per-rank across the whole run, i.e. the time
     one representative rank would spend on that operation if its calls
     ran back-to-back with no imbalance. This is an ESTIMATE, not a
     wall-clock measurement: it won't generally sum to EXACTLY the same
     total as the whole step's actual barrier-bounded wall-clock time
     (PDCregion_transfer_start_all_mpi through
     PDCregion_transfer_wait_all, including metadata ops, minus
     sleep(sleeptime) for async -- see vpic_bdcats.c's docstring), since
     that measurement captures cross-rank synchronization/imbalance this
     per-operation estimate doesn't. Figures 3/4 (write_total_time.png/
     read_total_time.png) plot that actual wall-clock total instead.
  3/4. Line (write_total_time.png, read_total_time.png), one figure per
     direction: total observed I/O time vs node count, one line per mode
     -- the actual barrier-bounded wall-clock measurement described above,
     not the per-operation estimate figures 1/2 are built from.
  5/6. Line (throughput_write.png, throughput_read.png), one figure per
     direction: aggregate throughput (total bytes moved / total observed
     time, not a mean of per-step rates) vs node count, one line per mode.

Async's sleep(sleeptime) between transfer start and wait is standing in
for compute overlapped with in-flight I/O and is already excluded from
every time/throughput value in the CSVs themselves (vpic_bdcats.c), so
sync and async are directly comparable here as real I/O cost, not
wall-clock time.

Usage:
    python3 plot_vpic_bdcats.py results_sync_XXXX results_async_XXXX -o plots/

Requires: numpy, matplotlib (no pandas) -- matches
pdc_helper_scripts/*/plot_*totals.py's convention.
"""
import argparse
import csv
import glob
import os
import re
from collections import defaultdict

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

MODE_ORDER = ["sync", "async"]
MODE_LINE_COLOR = {"sync": "#2a78d6", "async": "#eb6834"}
MODE_LABEL = {"sync": "Sync", "async": "Async"}

# Per-operation stacked bars (figures 4/5): fill = operation role (a
# validated-adjacent 6-hue categorical order, slots 3-8 of the dataviz
# skill's default palette -- slots 1-2, blue/orange, are reserved for
# mode identity elsewhere in this script, used here as each bar's EDGE
# color instead so operation (fill) and mode (edge) are two independent
# channels -- mirrors pdc_helper_scripts/*/plot_*totals.py's segment
# fill / workload edge convention). WRITE_OPS and READ_OPS share the
# same color BY POSITION for their analogous role (index 0 = "acquire
# object handle": PDCobj_create_mpi for write, PDCobj_open_col for read).
WRITE_OPS = [
    "PDCobj_create_mpi", "PDCregion_transfer_create", "PDCregion_transfer_start_all_mpi",
    "PDCregion_transfer_wait_all", "PDCregion_transfer_close", "PDCobj_close",
]
READ_OPS = [
    "PDCobj_open_col", "PDCregion_transfer_create", "PDCregion_transfer_start_all_mpi",
    "PDCregion_transfer_wait_all", "PDCregion_transfer_close", "PDCobj_close",
]
OP_ROLE_COLOR = ["#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
OP_ROLE_LABEL = [
    "object create/open", "transfer create", "transfer start_all_mpi",
    "transfer wait_all", "transfer close", "object close",
]
MODE_EDGE_COLOR = MODE_LINE_COLOR

_FILE_RE = re.compile(r"^vpic_bdcats_(sync|async)_(\d+)\.csv$")


def _fmt_count(n):
    """Compact human count: 214748352 -> '215M', 6871947264 -> '6.87B'."""
    n = float(n)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n / div:.3g}{suffix}"
    return f"{n:.0f}"


def parse_csv(path):
    """Return (rows, meta) -- rows is a list of dicts from the CSV body
    (record_type,name,step,mean_s,stdev_s,count,value), meta is the
    trailing `# key=value,...` comment line parsed into a dict."""
    meta = {}
    body_lines = []
    with open(path, newline="") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            if line.startswith("#"):
                for kv in line.lstrip("#").strip().split(","):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        meta[k.strip()] = v.strip()
            else:
                body_lines.append(line)
    rows = list(csv.DictReader(body_lines))
    return rows, meta


def load_all(dirs):
    """(mode, n_nodes) -> parsed run dict. When more than one
    vpic_bdcats_<mode>_<nodes>.csv exists for the same (mode, n_nodes) (a
    rerun), keep only the most-recently-modified file, matching
    plot_curl_totals.py's convention."""
    best_mtime = {}
    best_path = {}
    for d in dirs:
        for path in glob.glob(os.path.join(d, "vpic_bdcats_*.csv")):
            m = _FILE_RE.match(os.path.basename(path))
            if not m:
                continue
            key = (m.group(1), int(m.group(2)))
            mtime = os.path.getmtime(path)
            if key not in best_mtime or mtime > best_mtime[key]:
                best_mtime[key] = mtime
                best_path[key] = path

    runs = {}
    for key, path in best_path.items():
        rows, meta = parse_csv(path)
        mode = meta.get("mode")
        n_nodes = int(meta.get("n_nodes"))
        clients_per_node = int(meta.get("clients_per_node"))
        steps = int(meta.get("steps"))
        numparticles = int(meta.get("numparticles_per_rank"))
        ranks = clients_per_node * n_nodes
        particles_total = ranks * numparticles

        total_bytes = None
        for r in rows:
            if r["record_type"] == "total_data_size_bytes":
                total_bytes = float(r["value"])
                break
        if total_bytes is None:
            raise SystemExit(f"{path}: missing total_data_size_bytes row")
        bytes_per_step = total_bytes / steps
        gib = total_bytes / 1024**3

        per_step = {"write": {}, "read": {}}
        for r in rows:
            if r["record_type"] == "throughput_write_MBps":
                per_step["write"][int(r["step"])] = float(r["value"])
            elif r["record_type"] == "throughput_read_MBps":
                per_step["read"][int(r["step"])] = float(r["value"])

        time_s = {}
        agg_throughput_mbps = {}
        for direction in ("write", "read"):
            steps_seen = sorted(per_step[direction])
            total_time = sum((bytes_per_step / 1e6) / per_step[direction][s] for s in steps_seen)
            time_s[direction] = total_time
            # Aggregate throughput = total bytes moved / total observed
            # time, not a mean of per-step rates -- correct for
            # unevenly-timed steps, and what "total data / total time"
            # actually means.
            agg_throughput_mbps[direction] = (bytes_per_step * len(steps_seen) / 1e6) / total_time

        # Per-operation time estimate for the write/read stacked-by-
        # operation figures: mean_s * count / ranks -- see module
        # docstring for exactly what this does and doesn't represent.
        # Absent on CSVs from before the api_call_write/api_call_read
        # split (pooled "api_call" rows only) -- op_time stays empty in
        # that case, and plot_operation_bar skips runs with no data
        # rather than erroring.
        op_time = {"write": {}, "read": {}}
        for r in rows:
            if r["record_type"] == "api_call_write":
                op_time["write"][r["name"]] = float(r["mean_s"]) * float(r["count"]) / ranks
            elif r["record_type"] == "api_call_read":
                op_time["read"][r["name"]] = float(r["mean_s"]) * float(r["count"]) / ranks

        runs[key] = dict(
            mode=mode, n_nodes=n_nodes, ranks=ranks, particles_total=particles_total,
            gib=gib, steps=steps, time_s=time_s, throughput_mbps=agg_throughput_mbps,
            op_time=op_time,
        )
    return runs


def xtick_label(run):
    return (
        f"{run['n_nodes']} node{'s' if run['n_nodes'] != 1 else ''}\n"
        f"{run['ranks']} ranks\n"
        f"{_fmt_count(run['particles_total'])} particles\n"
        f"{run['gib']:.0f} GiB"
    )


def plot_total_time_line(runs, all_nodes, direction, out_path):
    """Line version of the observed-I/O-time data plot_operation_bar
    stacks by operation for one direction -- same time_s[direction]
    totals, plotted as a line vs scale instead of a bar. write and read
    each get their own figure, never combined."""
    n_groups = len(all_nodes)
    x = np.arange(n_groups)
    fig, ax = plt.subplots(figsize=(max(6.5, 1.4 * n_groups), 5.5), constrained_layout=True)

    for mode in MODE_ORDER:
        vals = np.array(
            [runs[(mode, n)]["time_s"][direction] if (mode, n) in runs else np.nan for n in all_nodes]
        )
        ax.plot(x, vals, marker="o", markersize=6, linewidth=2, color=MODE_LINE_COLOR[mode],
                label=MODE_LABEL[mode])

    ax.set_xticks(x)
    ax.set_xticklabels([xtick_label(runs[(MODE_ORDER[0], n)] if (MODE_ORDER[0], n) in runs
                                     else runs[(MODE_ORDER[1], n)]) for n in all_nodes], fontsize=8)
    ax.set_ylabel(f"Observed {direction} time (s)")
    ax.set_title(f"vpic_bdcats: {direction} time vs scale")
    ax.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax.set_axisbelow(True)
    ax.legend(fontsize=9)

    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Wrote {out_path}")


def plot_throughput_line(runs, all_nodes, direction, out_path):
    n_groups = len(all_nodes)
    x = np.arange(n_groups)
    fig, ax = plt.subplots(figsize=(max(6.5, 1.4 * n_groups), 5.5), constrained_layout=True)

    for mode in MODE_ORDER:
        vals = np.array(
            [runs[(mode, n)]["throughput_mbps"][direction] / 1e3 if (mode, n) in runs else np.nan
             for n in all_nodes]
        )
        ax.plot(x, vals, marker="o", markersize=6, linewidth=2,
                color=MODE_LINE_COLOR[mode], label=MODE_LABEL[mode])

    ax.set_xticks(x)
    ax.set_xticklabels([xtick_label(runs[(MODE_ORDER[0], n)] if (MODE_ORDER[0], n) in runs
                                     else runs[(MODE_ORDER[1], n)]) for n in all_nodes], fontsize=8)
    ax.set_ylabel(f"Aggregate {direction} throughput (GB/s)")
    ax.set_title(f"vpic_bdcats: {direction} throughput vs scale")
    ax.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax.set_axisbelow(True)
    ax.legend(fontsize=9)

    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Wrote {out_path}")


def plot_operation_bar(runs, all_nodes, direction, out_path):
    """Stacked bar for one direction (write or read): sync/async bars per
    node count, each stacked by PDC operation. See module docstring for
    the mean_s * count / ranks estimate this is built from."""
    ops = WRITE_OPS if direction == "write" else READ_OPS
    n_groups = len(all_nodes)
    group_width = 0.7
    bar_w = group_width / 2 * 0.85
    x = np.arange(n_groups)

    fig, ax = plt.subplots(figsize=(max(7.0, 1.6 * n_groups), 6.0), constrained_layout=True)

    any_data = False
    for mi, mode in enumerate(MODE_ORDER):
        offset = (mi - 0.5) * (group_width / 2)
        bottoms = np.zeros(n_groups)
        for oi, op in enumerate(ops):
            heights = np.array(
                [runs[(mode, n)]["op_time"][direction].get(op, 0.0) if (mode, n) in runs else 0.0
                 for n in all_nodes]
            )
            if np.any(heights > 0):
                any_data = True
            ax.bar(
                x + offset, heights, bar_w, bottom=bottoms,
                color=OP_ROLE_COLOR[oi], edgecolor=MODE_EDGE_COLOR[mode], linewidth=1.3,
                zorder=3,
            )
            bottoms += heights

    if not any_data:
        print(f"Skipping {out_path}: no api_call_{direction} rows found "
              f"(rerun the sweep with the current vpic_bdcats.c to get per-operation timing)")
        plt.close(fig)
        return

    ax.set_xticks(x)
    ax.set_xticklabels([xtick_label(runs[(MODE_ORDER[0], n)] if (MODE_ORDER[0], n) in runs
                                     else runs[(MODE_ORDER[1], n)]) for n in all_nodes], fontsize=8)
    ax.set_ylabel(f"Estimated {direction} time per operation (s), stacked")
    ax.set_title(f"vpic_bdcats: {direction} time by PDC operation (sync vs async)")
    ax.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax.set_axisbelow(True)

    op_handles = [plt.Rectangle((0, 0), 1, 1, facecolor=OP_ROLE_COLOR[i]) for i in range(len(ops))]
    leg1 = ax.legend(op_handles, OP_ROLE_LABEL, title="PDC operation", loc="upper left",
                      fontsize=8, title_fontsize=8)
    ax.add_artist(leg1)
    mode_handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor="white", edgecolor=MODE_EDGE_COLOR[m], linewidth=2.0)
        for m in MODE_ORDER
    ]
    ax.legend(mode_handles, [MODE_LABEL[m] for m in MODE_ORDER], title="mode", loc="upper right",
              fontsize=8, title_fontsize=8)

    # Headroom above the tallest bar so the top-left "PDC operation"
    # legend doesn't sit on top of a bar that happens to be tallest near
    # the left edge -- matches plot_curl_totals.py's convention.
    y_max = max(
        (sum(runs[(mode, n)]["op_time"][direction].values()) for mode in MODE_ORDER for n in all_nodes
         if (mode, n) in runs),
        default=1.0,
    )
    ax.set_ylim(0, y_max * 1.25)

    fig.savefig(out_path, dpi=200)
    plt.close(fig)
    print(f"Wrote {out_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dirs", nargs="*", default=["."],
                     help="Directories to search for vpic_bdcats_<mode>_<nodes>.csv (default: .)")
    ap.add_argument("-o", "--out-dir", default=".", help="Directory to write PNGs into (default: .)")
    args = ap.parse_args()

    runs = load_all(args.dirs)
    if not runs:
        raise SystemExit(f"No vpic_bdcats_<mode>_<nodes>.csv files found under {args.dirs}")

    all_nodes = sorted({n for (_, n) in runs})
    os.makedirs(args.out_dir, exist_ok=True)

    plot_total_time_line(runs, all_nodes, "write", os.path.join(args.out_dir, "write_total_time.png"))
    plot_total_time_line(runs, all_nodes, "read", os.path.join(args.out_dir, "read_total_time.png"))
    plot_throughput_line(runs, all_nodes, "write", os.path.join(args.out_dir, "throughput_write.png"))
    plot_throughput_line(runs, all_nodes, "read", os.path.join(args.out_dir, "throughput_read.png"))
    plot_operation_bar(runs, all_nodes, "write", os.path.join(args.out_dir, "write_stacked_bar.png"))
    plot_operation_bar(runs, all_nodes, "read", os.path.join(args.out_dir, "read_stacked_bar.png"))


if __name__ == "__main__":
    main()
