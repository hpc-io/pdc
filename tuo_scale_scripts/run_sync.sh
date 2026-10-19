#!/bin/bash
# Sweeps vpic_bdcats in "sync" transfer mode (start immediately followed
# by wait, no overlap) across NODE_COUNTS (common.sh; 64, 128 nodes --
# just the top two points of the original 1..128 progression), one
# sbatch job per node count, chained with --dependency=afterok so they
# run one at a time -- mirrors
# ../pdc_helper_scripts/curl_analysis_scripts/curl_eager_analysis_run.sh.
# This submits the whole chain and returns immediately; check progress
# with `squeue -u $USER`.
#
# Run tuo_scale_scripts/run_small_scale.sh first to sanity-check the whole
# pipeline (server, client, CSV extraction) on a single node before
# committing to this full sweep.
#
# Required: export PDC_DATA_LOC to real parallel scratch first (see
# common.sh) -- this sweep's 128-node step alone writes the full 4 TiB
# target (64 nodes writes half that, ~2 TiB -- see common.sh).

set -eu
cd "$(dirname "$0")"
source ./common.sh

export MODE=sync
export RESULTS_DIR=${RESULTS_DIR:-$PWD/results_sync_$(date +%Y%m%d_%H%M%S)}
mkdir -p "$RESULTS_DIR"
echo "Results will land in: $RESULTS_DIR"

prev_jid=""
for nodes in "${NODE_COUNTS[@]}"; do
    echo "=== [sync] Submitting vpic_bdcats scale job: $nodes node(s) ==="
    export NUM_NODES="$nodes"
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --parsable \
                --job-name="vpic-bdcats-sync-${nodes}" \
                --nodes="$nodes" \
                --ntasks-per-node="$TASKS_PER_NODE" \
                --cpus-per-task="$CPUS_PER_TASK" \
                vpic_bdcats.sbatch)
    else
        jid=$(sbatch --parsable \
                --job-name="vpic-bdcats-sync-${nodes}" \
                --nodes="$nodes" \
                --ntasks-per-node="$TASKS_PER_NODE" \
                --cpus-per-task="$CPUS_PER_TASK" \
                --dependency=afterok:$prev_jid \
                vpic_bdcats.sbatch)
    fi
    echo "Submitted job $jid ($nodes node(s))"
    prev_jid=$jid
done

echo "All jobs submitted, chained via --dependency=afterok. Check progress"
echo "with: squeue -u \$USER"
echo "Per-node-count CSVs will land in $RESULTS_DIR as each job completes."
