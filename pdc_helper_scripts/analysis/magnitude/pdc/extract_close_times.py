#!/usr/bin/env python3
"""
Recompute avg_close_s / total_with_close_s for every results_<mode>_*.csv
in a results directory, from the close_server_<mode>_<num_nodes>.log
files each job's srun_close_server.sh step leaves in BIN_DIR (see
srun_close_server.sh -- it `pushd`s into BIN_DIR before running srun, so
--output/--error land there, not in SLURM_SUBMIT_DIR).

This exists because:
  - eager_pdc.sbatch / lazy_pdc.sbatch / posthoc_pdc.sbatch /
    posthoc_hdf5.sbatch now add these two columns themselves at the end
    of each job (see README.md's "Result CSV schema" section), but any
    results CSV generated before that change, or copied down before that
    job's own post-processing step ran, won't have them -- this
    recomputes and merges them in after the fact.
  - close_server_<mode>_<num_nodes>.log has no jobid in its filename (see
    srun_close_server.sh), so it always reflects the MOST RECENT close at
    that (mode, node count) -- rerunning this script after a fresh sweep
    just picks up whatever's currently sitting in BIN_DIR.

Re-running this script is idempotent: it strips any avg_close_s /
total_with_close_s columns a results CSV already has before recomputing
them, rather than stacking duplicate columns on repeated runs.

Usage:
    python3 extract_close_times.py [--bin-dir DIR] [--results-dir DIR]

No third-party dependencies (stdlib only).
"""
import argparse
import csv
import glob
import os
import re

# Constant across eager_pdc.sbatch / lazy_pdc.sbatch / posthoc_pdc.sbatch /
# posthoc_hdf5.sbatch (CLIENTS_PER_NODE=32 in every one of them), so
# num_nodes is always recoverable from a results row's n_ranks column.
CLIENTS_PER_NODE = 32

CLOSE_LOG_RE = re.compile(r"^close_server_(?P<mode>[A-Za-z0-9]+)_(?P<nodes>\d+)\.log$")
CLOSE_TIME_RE = re.compile(r"total close time = ([0-9.]+)")


def scan_close_logs(bin_dir):
    """{(mode, num_nodes): (avg_close_s, n_samples)} from every
    close_server_<mode>_<num_nodes>.log under bin_dir -- one "total close
    time = X" line per server rank, printed by close_server.c."""
    out = {}
    for path in glob.glob(os.path.join(bin_dir, "close_server_*.log")):
        m = CLOSE_LOG_RE.match(os.path.basename(path))
        if not m:
            continue
        mode = m.group("mode")
        num_nodes = int(m.group("nodes"))
        vals = []
        with open(path, errors="replace") as f:
            for line in f:
                cm = CLOSE_TIME_RE.search(line)
                if cm:
                    vals.append(float(cm.group(1)))
        if vals:
            out[(mode, num_nodes)] = (sum(vals) / len(vals), len(vals))
    return out


def write_close_times_csv(close_times, out_path):
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["mode", "num_nodes", "avg_close_s", "n_samples"])
        for (mode, num_nodes), (avg, n) in sorted(close_times.items()):
            w.writerow([mode, num_nodes, f"{avg:.6f}", n])


def _strip_existing_close_cols(header, rows):
    """Drop avg_close_s/total_with_close_s from header and every full-width
    row if already present, so re-running this script doesn't stack
    duplicate columns. Order matters: total_with_close_s sits to the
    right of avg_close_s, so removing it first keeps avg_close_s's index
    valid for the second removal."""
    n_fields = len(header)
    for extra in ("total_with_close_s", "avg_close_s"):
        if extra in header:
            idx = header.index(extra)
            header = header[:idx] + header[idx + 1:]
            for i in range(len(rows)):
                if len(rows[i]) == n_fields:
                    rows[i] = rows[i][:idx] + rows[i][idx + 1:]
            n_fields -= 1
    return header, rows


def update_results_csv(path, close_times):
    """Rewrite one results_<mode>_<jobid>.csv in place, (re)setting its
    trailing avg_close_s,total_with_close_s columns from close_times.
    FAILED sentinel rows (short row -- see srun_client_eager.sh etc.) are
    left untouched. hdf5 rows always get avg_close_s=0 (no server to
    close)."""
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return False
    header, rows = rows[0], rows[1:]
    header, rows = _strip_existing_close_cols(header, rows)

    total_col_name = "step_total_s" if "step_total_s" in header else "total_s"
    if total_col_name not in header or "n_ranks" not in header:
        return False
    total_idx = header.index(total_col_name)
    n_ranks_idx = header.index("n_ranks")
    n_fields = len(header)

    new_rows = [header + ["avg_close_s", "total_with_close_s"]]
    updated = False
    for row in rows:
        if len(row) != n_fields:
            new_rows.append(row)  # FAILED sentinel row, pass through as-is
            continue
        mode = row[0]
        try:
            n_ranks = int(row[n_ranks_idx])
            total = float(row[total_idx])
        except ValueError:
            new_rows.append(row)
            continue
        if mode == "hdf5":
            avg_close = 0.0
        else:
            avg_close, _ = close_times.get((mode, n_ranks // CLIENTS_PER_NODE), (0.0, 0))
        new_rows.append(row + [f"{avg_close:.6f}", f"{total + avg_close:.6f}"])
        updated = True

    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(new_rows)
    return updated


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument(
        "--bin-dir", default=os.path.join(here, "..", "..", "build", "bin"),
        help="Directory holding close_server_<mode>_<num_nodes>.log (default: ../../build/bin)",
    )
    ap.add_argument(
        "--results-dir", default=here,
        help="Directory holding results_<mode>_*.csv, and where close_times.csv is written (default: this script's directory)",
    )
    args = ap.parse_args()

    bin_dir = os.path.abspath(args.bin_dir)
    results_dir = os.path.abspath(args.results_dir)

    close_times = scan_close_logs(bin_dir)
    if not close_times:
        print(f"No close_server_*.log files with 'total close time' lines found under {bin_dir}")
    else:
        for (mode, num_nodes), (avg, n) in sorted(close_times.items()):
            print(f"{mode:8s} {num_nodes:4d} nodes  avg_close_s={avg:.6f}  (n={n})")

    out_csv = os.path.join(results_dir, "close_times.csv")
    write_close_times_csv(close_times, out_csv)
    print(f"Wrote {out_csv}")

    n_updated = 0
    for path in sorted(glob.glob(os.path.join(results_dir, "results_*.csv"))):
        if update_results_csv(path, close_times):
            n_updated += 1
            print(f"Updated {path}")
    print(f"Updated {n_updated} results CSV file(s)")


if __name__ == "__main__":
    main()
