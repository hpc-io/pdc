#!/bin/bash
# In-flight analog of curl_analysis.sbatch -- same node-count-per-job
# shape, 32 client ranks/node, no server, but runs
# adios2_bench_curl_inflight (curl manual + vorticity_magnitude as a
# registered derived variable, both inside the write path) instead of
# adios2_bench_curl (posthoc).
#
# Submit one of these per node count via curl_inflight_analysis_run.sh,
# or directly, e.g.: sbatch --nodes=4 curl_inflight_analysis.sbatch
# Override NX/NY/NZ_PER_RANK at submit time, e.g.:
#   NZ_PER_RANK=128 sbatch --nodes=4 curl_inflight_analysis.sbatch

#SBATCH --job-name=adios2-curl-inflight
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=32
#SBATCH --cpus-per-task=4
#SBATCH --account=m2621
#SBATCH --time=00:30:00
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --output=%x_%j.log

set -xeu
cd "$SLURM_SUBMIT_DIR"

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENTS_PER_NODE=32
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD}
export OUT_DIR=${OUT_DIR:-$SCRATCH/adios2_analysis_bench}
mkdir -p "$OUT_DIR"

# Same per-rank shape as curl_analysis.sbatch's own defaults, so the two
# modes are directly comparable.
export NX=${NX:-256}
export NY=${NY:-256}
export NZ_PER_RANK=${NZ_PER_RANK:-64}
export OUT_FILE=${OUT_DIR}/adios2_bench_curl_inflight_${NUM_NODES}.bp
export RESULTS=$PWD/results_adios2_curl_inflight_${SLURM_JOB_ID}.csv

STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((NX * NY * NZ_PER_RANK * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
rm -rf "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_DIR" \
  || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,nx,ny,nz_per_rank,setup_s,write_s,confirm_read_s,close_s,step_total_s,bad" > "$RESULTS"

srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_curl_inflight_${NUM_NODES}.log" \
  --error="client_curl_inflight_${NUM_NODES}.err" \
  "$BIN_DIR/adios2_bench_curl_inflight" "$NX" "$NY" "$NZ_PER_RANK" "$OUT_FILE"

grep "^adios2_inflight_curl," "client_curl_inflight_${NUM_NODES}.log" >> "$RESULTS" || true

# Prepend servers_per_node as the first column (0 -- no PDC server
# involved; compute runs on the client ranks themselves, see
# adios2_bench_curl_inflight.cpp's header comment).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
