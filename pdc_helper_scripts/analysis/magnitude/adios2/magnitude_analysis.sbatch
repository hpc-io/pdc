#!/bin/bash
# ADIOS2 analog of pdc_helper_scripts/analysis_scripts/eager_pdc.sbatch
# -- single node-count step, 32 client ranks/node, no server (ADIOS2 has
# none: this writes straight to Lustre-backed BP5 files). Same N_ELEM
# per rank as the real eager_pdc.sbatch, so results are directly
# comparable.
#
# Submit one of these per node count via magnitude_analysis_run.sh, or
# directly, e.g.: sbatch --nodes=4 magnitude_analysis.sbatch
# Override N_ELEM at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=4 magnitude_analysis.sbatch

#SBATCH --job-name=adios2-magnitude
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

# Same 64 MiB/rank float32 as eager_pdc.sbatch's own N_ELEM default --
# see that file's comment for the full weak-scaling budget derivation
# (960 MiB/rank across N_TIMESTEPS=3, ~3.75 TiB aggregate at the 128-node
# top of this sweep).
export N_ELEM=${N_ELEM:-16777216}
export OUT_FILE=${OUT_DIR}/adios2_bench_magnitude_${NUM_NODES}.bp
export RESULTS=$PWD/results_adios2_magnitude_${SLURM_JOB_ID}.csv

# Lustre striping, same convention as posthoc_hdf5.sbatch: stripe size
# == one rank's per-write payload, striped across all 128 OSTs, so the
# ADIOS2 baseline isn't artificially bottlenecked onto one OST the way
# an unstriped new file defaults to.
STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((N_ELEM * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
rm -rf "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_DIR" \
  || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,n_elem,setup_s,write_s,confirm_read_s,close_s,step_total_s,bad" > "$RESULTS"

srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_magnitude_${NUM_NODES}.log" \
  --error="client_magnitude_${NUM_NODES}.err" \
  "$BIN_DIR/adios2_bench_magnitude" "$N_ELEM" "$OUT_FILE"

grep "^adios2_derived," "client_magnitude_${NUM_NODES}.log" >> "$RESULTS" || true


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
