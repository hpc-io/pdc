#!/bin/bash
# Same as magnitude_analysis.sbatch, but with BP5's AggregationType
# forced to "EveryoneWrites" instead of the default "TwoLevelShm" --
# every rank writes its own data independently, no shared-memory
# aggregator stage at all. See adios2_bench_magnitude.cpp's
# ADIOS2_AGGREGATION_TYPE handling: BP5's default is a "fewer ranks
# actually touch storage" aggregation strategy (comparable in spirit to
# why HDF5's collective I/O helps, though a genuinely different
# mechanism -- BP5 never uses MPI-IO collective calls), so this script
# exists to give a true independent-I/O comparison point against that
# default, not to replace it.
#
# Submit one of these per node count via
# magnitude_analysis_everyonewrites_run.sh, or directly, e.g.:
#   sbatch --nodes=4 magnitude_analysis_everyonewrites.sbatch
# Override N_ELEM at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=4 magnitude_analysis_everyonewrites.sbatch

#SBATCH --job-name=adios2-magnitude-ew
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
export OUT_DIR=${OUT_DIR:-$SCRATCH/adios2_analysis_bench_ew}
mkdir -p "$OUT_DIR"

# Same 64 MiB/rank float32 as magnitude_analysis.sbatch's own N_ELEM
# default, so results are directly comparable against the default
# (TwoLevelShm) run.
export N_ELEM=${N_ELEM:-16777216}
export OUT_FILE=${OUT_DIR}/adios2_bench_magnitude_ew_${NUM_NODES}.bp
export RESULTS=$PWD/results_adios2_magnitude_everyonewrites_${SLURM_JOB_ID}.csv
export ADIOS2_AGGREGATION_TYPE=EveryoneWrites

# Lustre striping, same convention as magnitude_analysis.sbatch /
# posthoc_hdf5.sbatch: stripe size == one rank's per-write payload,
# striped across all 128 OSTs. With every rank now writing
# independently (no aggregators funneling I/O through fewer ranks),
# getting this right matters even more than in the default-aggregation
# run -- an unstriped file would bottleneck all CLIENT_TOTAL_TASKS ranks
# onto one OST instead of just the (far fewer) aggregators.
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
  --output="client_magnitude_ew_${NUM_NODES}.log" \
  --error="client_magnitude_ew_${NUM_NODES}.err" \
  "$BIN_DIR/adios2_bench_magnitude" "$N_ELEM" "$OUT_FILE"

grep "^adios2_derived," "client_magnitude_ew_${NUM_NODES}.log" >> "$RESULTS" || true


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
