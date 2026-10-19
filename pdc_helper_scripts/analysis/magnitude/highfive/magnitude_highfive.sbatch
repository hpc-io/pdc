#!/bin/bash
# HighFive (C++ wrapper over the same libhdf5 posthoc_hdf5.sbatch already
# benchmarks) magnitude analysis baseline -- single node-count step. No
# PDC server involved; see hdf5_analysis_test/magnitude_highfive.cpp.
#
# Unlike posthoc_hdf5.sbatch, this is one continuous process (write
# vx/vy/vz, read back, compute magnitude, write it back, confirm-read),
# not a write-then-relaunch-then-analyze pair -- see that file's own
# header comment for why (it exists to measure the C++ wrapper's own
# overhead, not to model a posthoc workflow a second time). One srun
# step, one CSV row per timestep, straight to results_magnitude_highfive_<jobid>.csv.
#
# Submit one of these per node count via magnitude_highfive_run.sh, or
# directly, e.g.: sbatch --nodes=4 magnitude_highfive.sbatch
# Override N_ELEM at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=4 magnitude_highfive.sbatch
#
# Needs the same parallel-HDF5 module that built hdf5_analysis_test/ (see
# hdf5_analysis_test/README.md) loaded here too -- the compute-node
# environment doesn't inherit whatever was loaded in the login shell at
# build time, so the module load below is required even though the
# binary was already built.

#SBATCH --job-name=hdf5-magnitude-highfive
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=32
#SBATCH --cpus-per-task=4
#SBATCH --account=m2621
#SBATCH --time=00:30:00
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --output=%x_%j.log

set -u
cd "$SLURM_SUBMIT_DIR"

module load cray-hdf5-parallel

# CLIENTS_PER_NODE, N_ELEM: see ../common.sh (no PDC server here, so
# SERVERS_PER_NODE from that file is unused). This file's own #SBATCH
# --ntasks-per-node above must still equal CLIENTS_PER_NODE.
source "$SLURM_SUBMIT_DIR/../../common.sh"

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD}
export OUT_DIR=${OUT_DIR:-$SCRATCH/hdf5_analysis_bench}
mkdir -p "$OUT_DIR"

export RESULTS=$PWD/results_magnitude_highfive_${SLURM_JOB_ID}.csv
export OUT_FILE=${OUT_DIR}/magnitude_highfive_${NUM_NODES}.h5

# Lustre stripe size == exactly what one rank writes per collective write
# (N_ELEM float32 elements), rounded up to the required 64 KiB stripe
# granularity, striped across all 128 OSTs -- same rationale as
# posthoc_hdf5.sbatch's own striping (see that file's comment).
STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((N_ELEM * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
echo "Lustre striping: count=${STRIPE_COUNT} size=${STRIPE_SIZE_BYTES} bytes ($((STRIPE_SIZE_BYTES / 1048576)) MiB/rank), target=${OUT_FILE}"

# Lustre striping can only be set at file-creation time -- see
# posthoc_hdf5.sbatch's identical comment on why any stale file has to be
# removed first.
rm -f "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_FILE" || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,n_elem,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,close_s,step_total_s,bad" > "$RESULTS"

./srun_magnitude_highfive.sh


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
