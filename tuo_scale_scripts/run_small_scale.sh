#!/bin/bash
# Small, fast sanity check of the whole pipeline (sbatch submission,
# pdc_server startup, vpic_bdcats write+read-back, CSV extraction) on
# 2 nodes with a tiny particle count -- run this BEFORE run_sync.sh /
# run_async.sh to catch config problems (bad PDC_DATA_LOC, a partition/
# account mismatch in vpic_bdcats.sbatch, a cross-node transport issue
# like Tuolumne's cxi-only libfabric, etc.) without waiting on a real
# node-count sweep or writing anything close to the full sweep's ~4 TiB.
# 2 nodes (not 1) specifically because some issues -- e.g. the ofi+tcp
# vs ofi+cxi transport mismatch fixed in srun_server.sh/
# srun_client_vpic_bdcats.sh/srun_close_server.sh -- only showed up once
# the server/client actually had to communicate across node boundaries.
#
# Runs both sync and async (short sleep) modes, 2 nodes, 2 timesteps,
# 1024 particles/rank -- seconds, not the tens of minutes a real sweep
# step takes. Uses `sbatch --wait` (rather than run_sync.sh/run_async.sh's
# --dependency=afterok chaining) so this script can block and print each
# CSV immediately after that mode's job finishes.

set -eu
cd "$(dirname "$0")"

# Deliberately override common.sh's weak-scaling NUMPARTICLES/STEPS with
# tiny fixed values for this debug run, not the real sweep sizing.
export CLIENTS_PER_NODE=${CLIENTS_PER_NODE:-4}
export NUMPARTICLES=1024
export STEPS=2
source ./common.sh

export RESULTS_DIR=${RESULTS_DIR:-$PWD/results_small_scale_$(date +%Y%m%d_%H%M%S)}
mkdir -p "$RESULTS_DIR"
echo "Results will land in: $RESULTS_DIR"

NODES=2
export NUM_NODES="$NODES"

for MODE in sync async; do
    export MODE
    export ASYNC_SLEEP_S=1
    echo "=== [small-scale, ${MODE}] Submitting vpic_bdcats debug job: ${NODES} node ==="
    sbatch --wait \
      --job-name="vpic-bdcats-small-${MODE}" \
      --nodes="$NODES" \
      --ntasks-per-node="$TASKS_PER_NODE" \
      --cpus-per-task="$CPUS_PER_TASK" \
      vpic_bdcats.sbatch
    echo "Finished ${MODE}"
    echo "--- $(cat "${RESULTS_DIR}/vpic_bdcats_${MODE}_${NODES}.csv" | head -20) ---"
done

echo "Done. If both modes above show a clean CSV (api_call rows, throughput"
echo "rows, no mismatches in ${RESULTS_DIR}/client_*.log), the pipeline is"
echo "good to go -- proceed to run_sync.sh / run_async.sh."
