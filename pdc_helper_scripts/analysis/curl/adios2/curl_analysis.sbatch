#!/bin/bash
# ADIOS2 analog of
# pdc_helper_scripts/curl_analysis_scripts/curl_eager_analysis.sbatch --
# single node-count step, 32 client ranks/node, no server. Same
# NX/NY/NZ_PER_RANK per rank as the real curl_eager_analysis.sbatch.
#
# Submit one of these per node count via curl_analysis_run.sh, or
# directly, e.g.: sbatch --nodes=4 curl_analysis.sbatch
# Override NX/NY/NZ_PER_RANK at submit time, e.g.:
#   NZ_PER_RANK=128 sbatch --nodes=4 curl_analysis.sbatch

#SBATCH --job-name=adios2-curl
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

# Same per-rank shape as curl_eager_analysis.sbatch's own defaults --
# see pdc_helper_scripts/curl_analysis_scripts/README.md for how this
# was derived from the source paper's reported data volumes.
export NX=${NX:-256}
export NY=${NY:-256}
export NZ_PER_RANK=${NZ_PER_RANK:-64}
export OUT_FILE=${OUT_DIR}/adios2_bench_curl_${NUM_NODES}.bp
export RESULTS=$PWD/results_adios2_curl_${SLURM_JOB_ID}.csv

STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((NX * NY * NZ_PER_RANK * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
rm -rf "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_DIR" \
  || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,nx,ny,nz_per_rank,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,step_total_s,bad" > "$RESULTS"

srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_curl_${NUM_NODES}.log" \
  --error="client_curl_${NUM_NODES}.err" \
  "$BIN_DIR/adios2_bench_curl" "$NX" "$NY" "$NZ_PER_RANK" "$OUT_FILE"

grep "^adios2_posthoc_curl," "client_curl_${NUM_NODES}.log" >> "$RESULTS" || true


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
