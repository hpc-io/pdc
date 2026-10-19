#!/usr/bin/env python3
"""
One-off backfill: prepend a servers_per_node column to every already-
committed result CSV under pdc_helper_scripts/analysis/, .../
transformation/adios2_compression/, and .../io/adios2_vpicio/, so old
data has the same column new sbatch runs now emit automatically (see
each sbatch script's own "Prepend servers_per_node" sed line).

Inference:
- Any CSV under a "*/pdc/" subtree: server count taken from the nearest
  ancestor directory matching "^<N>_servers" (2_servers_09_25_2025/,
  8_servers_09_25_2026/, etc.). Files with no such ancestor (loose files
  directly in pdc/, or under csv_res//previous_results/) default to 8,
  matching this project's historical posthoc convention -- flagged below
  since it's an assumption, not read from anything.
- Any CSV under a "*/hdf5/", "*/adios2/", or "*/highfive/" subtree: 0
  (no PDC server involved by definition).

Idempotent: skips any file whose header already starts with
"servers_per_node".

Usage: python3 backfill_servers_per_node.py [--dry-run]
"""
import csv
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PDC_HELPER = REPO_ROOT / "pdc_helper_scripts"

SERVER_DIR_RE = re.compile(r"^(\d+)_servers")
CSV_NAME_RE = re.compile(r"^(results|write|analyze)_.*\.csv$")

ROOTS = [
    PDC_HELPER / "analysis" / "magnitude" / "pdc",
    PDC_HELPER / "analysis" / "magnitude" / "hdf5",
    PDC_HELPER / "analysis" / "magnitude" / "adios2",
    PDC_HELPER / "analysis" / "magnitude" / "highfive",
    PDC_HELPER / "analysis" / "curl" / "pdc",
    PDC_HELPER / "analysis" / "curl" / "hdf5",
    PDC_HELPER / "analysis" / "curl" / "adios2",
    PDC_HELPER / "transformation" / "adios2_compression",
    PDC_HELPER / "io" / "adios2_vpicio",
]

NO_SERVER_DIRNAMES = {"hdf5", "highfive", "adios2", "adios2_compression", "adios2_vpicio"}
DEFAULT_ASSUMED_SERVERS = 8


def infer_servers(csv_path: Path, root: Path):
    """Returns (value, assumed: bool)."""
    if root.name in NO_SERVER_DIRNAMES:
        return 0, False
    # root.name == "pdc": walk up from the file looking for a <N>_servers* dir
    for parent in csv_path.parents:
        if parent == root.parent:
            break
        m = SERVER_DIR_RE.match(parent.name)
        if m:
            return int(m.group(1)), False
    return DEFAULT_ASSUMED_SERVERS, True


def backfill_file(path: Path, value: int, dry_run: bool):
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        return False
    if rows[0] and rows[0][0] == "servers_per_node":
        return False  # already backfilled
    new_rows = [["servers_per_node"] + rows[0]]
    for r in rows[1:]:
        new_rows.append([str(value)] + r)
    if not dry_run:
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerows(new_rows)
    return True


def main():
    dry_run = "--dry-run" in sys.argv
    changed = 0
    assumed = []
    for root in ROOTS:
        if not root.is_dir():
            print(f"NOTE: {root} does not exist, skipping")
            continue
        for csv_path in sorted(root.rglob("*.csv")):
            if not CSV_NAME_RE.match(csv_path.name):
                continue
            value, was_assumed = infer_servers(csv_path, root)
            did = backfill_file(csv_path, value, dry_run)
            if did:
                changed += 1
                tag = " (ASSUMED, no <N>_servers* ancestor dir)" if was_assumed else ""
                print(f"{'[dry-run] ' if dry_run else ''}{csv_path.relative_to(REPO_ROOT)}: servers_per_node={value}{tag}")
                if was_assumed:
                    assumed.append(str(csv_path.relative_to(REPO_ROOT)))

    print(f"\n{'Would change' if dry_run else 'Changed'} {changed} files.")
    if assumed:
        print(f"\n{len(assumed)} file(s) had NO <N>_servers* ancestor directory -- assumed {DEFAULT_ASSUMED_SERVERS}:")
        for a in assumed:
            print(f"  {a}")


if __name__ == "__main__":
    main()
