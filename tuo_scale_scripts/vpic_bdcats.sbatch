#!/bin/bash
# vpic_bdcats scaling job for one node count, one transfer mode --
# submitted via `sbatch --nodes=N ...` by run_sync.sh / run_async.sh /
# run_small_scale.sh, never invoked directly. See
# src/tests/misc/vpic_bdcats.c for what the benchmark itself does (write 5
# timesteps, then read every one back in the same process so the server's
# region cache is still warm).
#
# Mirrors pdc_helper_scripts/curl_analysis_scripts/curl_eager_analysis.sbatch
# exactly: one sbatch allocation sized to SERVERS_PER_NODE +
# CLIENTS_PER_NODE tasks/node, a backgrounded (`&`) srun step for the
# server and a foreground srun step for the client, both able to run
# concurrently because Slurm's default job-step semantics let multiple
# srun steps within one sbatch allocation share it, as long as their
# combined per-node task request doesn't exceed --ntasks-per-node. This
# replaces an earlier Flux (`flux batch`/`flux run`/`flux submit`)
# implementation in this directory that never got the two steps
# co-scheduled even with a whole exclusive node reserved -- see git
# history for that attempt. --nodes/--ntasks-per-node/--cpus-per-task
# below are just defaults for direct manual submission; run_*.sh always
# overrides them from common.sh's SERVERS_PER_NODE/CLIENTS_PER_NODE.
#
# Required env (set by the calling run_*.sh, which sources common.sh):
#   NUM_NODES, SERVERS_PER_NODE, CLIENTS_PER_NODE, STEPS, NUMPARTICLES,
#   MODE (sync|async), PDC_DATA_LOC, PDC_TMPDIR, BIN_DIR, RESULTS_DIR
# Additional required env when MODE=async:
#   ASYNC_SLEEP_S
#
# NUM_NODES is passed explicitly by the caller rather than read from
# SLURM_JOB_NUM_NODES -- Tuolumne's sbatch/srun are Flux's own Slurm-
# compatibility wrappers (its rc2 runs this script directly as a Flux
# job), not real Slurm, and that shim doesn't populate SLURM_JOB_NUM_NODES
# (confirmed: "line 45: SLURM_JOB_NUM_NODES: unbound variable" on a real
# run). Every run_*.sh already knows the node count it's submitting with,
# so there's no need to recover it from a Slurm-specific env var this
# shim doesn't set.

#SBATCH --job-name=vpic-bdcats-scale
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=36
#SBATCH --cpus-per-task=2
#SBATCH --partition=pbatch
#SBATCH --time=00:30:00
#SBATCH --output=%x_%j.log

set -xeu
# Tuolumne's sbatch/srun are Flux's own Slurm-compatibility wrappers (its
# rc2 runs this script directly as a Flux job), not real Slurm -- Flux's
# shim doesn't populate SLURM_SUBMIT_DIR the way real Slurm does. The job
# already starts in the submission directory under this shim, so fall
# back to $PWD instead of failing on the unset var.
cd "${SLURM_SUBMIT_DIR:-$PWD}"

NUM_NODES=${NUM_NODES:?}
export NUM_NODES
SERVERS_PER_NODE=${SERVERS_PER_NODE:?}
CLIENTS_PER_NODE=${CLIENTS_PER_NODE:?}
export SERVERS_PER_NODE CLIENTS_PER_NODE
export SERVER_TOTAL_TASKS=$((NUM_NODES * SERVERS_PER_NODE))
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export STEPS=${STEPS:?}
export NUMPARTICLES=${NUMPARTICLES:?}
export MODE=${MODE:?MODE must be sync or async}
export ASYNC_SLEEP_S=${ASYNC_SLEEP_S:-0}

export BIN_DIR=${BIN_DIR:?}
export PDC_DATA_LOC=${PDC_DATA_LOC:?}
export PDC_TMPDIR=${PDC_TMPDIR:-$PDC_DATA_LOC}
export RESULTS_DIR=${RESULTS_DIR:?}
mkdir -p "$RESULTS_DIR" "$PDC_DATA_LOC"

export LOG_TAG="vpic_bdcats_${MODE}"
OUT_CSV="${RESULTS_DIR}/vpic_bdcats_${MODE}_${NUM_NODES}.csv"
export OUT_CSV

./srun_server.sh
./srun_client_vpic_bdcats.sh
./srun_close_server.sh

# Sanity-check that data actually landed on disk, not just in the
# server's in-memory region cache -- this has to run AFTER
# srun_close_server.sh, since close_server's RPC is what flushes that
# cache to the storage backend (see srun_close_server.sh's own comment).
# PDC writes object data under $PDC_DATA_LOC/pdc_data/<obj_id>/... (see
# src/server/pdc_server_region/pdc_server_data.c:112-113), matching
# pdc_helper_scripts/curl_analysis_scripts/curl_eager_analysis.sbatch's
# own du -sb check.
BYTES_PER_PARTICLE=32   # 7 floats + 1 int (dX,dY,dZ,Ux,Uy,Uz,q,i) -- matches src/tests/misc/vpic_bdcats.c
EXPECTED_BYTES=$((CLIENT_TOTAL_TASKS * NUMPARTICLES * BYTES_PER_PARTICLE * STEPS))
ACTUAL_BYTES=$(du -sb "$PDC_DATA_LOC/pdc_data" 2>/dev/null | awk '{print $1}')
ACTUAL_BYTES=${ACTUAL_BYTES:-0}
echo "PDC data dir size: ${ACTUAL_BYTES} bytes (expected ~${EXPECTED_BYTES} bytes)"
if [ "$ACTUAL_BYTES" -eq 0 ]; then
    echo "ERROR: $PDC_DATA_LOC/pdc_data is empty -- no data was actually written to disk" >&2
    exit 1
fi
awk -v e="$EXPECTED_BYTES" -v a="$ACTUAL_BYTES" 'BEGIN {
    ratio = a / e
    if (ratio < 0.95 || ratio > 1.05)
        printf "WARNING: actual/expected data size ratio %.3f is outside [0.95, 1.05] -- possible incomplete write or stale data directory\n", ratio
    else
        printf "OK: actual/expected data size ratio %.3f within tolerance\n", ratio
}'
echo "data_dir_size_bytes,,,,,,${ACTUAL_BYTES}" >> "$OUT_CSV"

echo "# n_nodes=${NUM_NODES},servers_per_node=${SERVERS_PER_NODE},clients_per_node=${CLIENTS_PER_NODE},mode=${MODE},steps=${STEPS},numparticles_per_rank=${NUMPARTICLES},async_sleep_s=${ASYNC_SLEEP_S}" >> "$OUT_CSV"

echo "Wrote $OUT_CSV"
