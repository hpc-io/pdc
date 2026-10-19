#!/bin/bash
# Parallel-HDF5 magnitude analysis baseline -- single node-count step. No
# PDC server involved; see hdf5_analysis_test/. 32 client ranks/node,
# writing one CSV row to results_hdf5_<jobid>.csv.
#
# Mirrors the PDC posthoc benchmark's two-phase shape: write vx/vy/vz in
# one srun step, let every rank fully exit, then a separate srun step
# reopens the file, reads back, computes magnitude, and writes it back --
# instead of one continuous process doing all of it, so the real cost of
# relaunching between the write and the analysis is measured rather than
# hidden.
#
# Submit one of these per node count via posthoc_hdf5_run.sh, or
# directly, e.g.: sbatch --nodes=4 posthoc_hdf5.sbatch
# Override N_ELEM at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=4 posthoc_hdf5.sbatch
#
# Needs the same parallel-HDF5 module that built hdf5_analysis_test/ (see
# hdf5_analysis_test/README.md) loaded here too -- the compute-node
# environment doesn't inherit whatever was loaded in the login shell at
# build time, so the module load below is required even though the
# binary was already built.

#SBATCH --job-name=hdf5-magnitude
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

export LOG_TAG=hdf5
export RESULTS=$PWD/results_hdf5_${SLURM_JOB_ID}.csv
export OUT_FILE=${OUT_DIR}/hdf5_bench_magnitude_${NUM_NODES}.h5
export WRITE_LOG=$PWD/write_${LOG_TAG}_${SLURM_JOB_ID}.csv
export ANALYZE_LOG=$PWD/analyze_${LOG_TAG}_${SLURM_JOB_ID}.csv

# Lustre stripe size == exactly what one rank writes per collective write
# (N_ELEM float32 elements), rounded up to the required 64 KiB stripe
# granularity, striped across all 128 OSTs on this filesystem. Without
# this, new files default to a single OST, so the one shared HDF5 file's
# collective I/O funnels through one disk target regardless of rank
# count. PDC's servers similarly stripe their own data directory onto
# dedicated OSTs (see src/server/pdc_server_region/pdc_server_data.c),
# so this keeps the HDF5 baseline comparable instead of artificially
# bottlenecked.
STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((N_ELEM * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
echo "Lustre striping: count=${STRIPE_COUNT} size=${STRIPE_SIZE_BYTES} bytes ($((STRIPE_SIZE_BYTES / 1048576)) MiB/rank), target=${OUT_FILE}"

# Lustre striping can only be set at file-creation time, so any stale
# file at this path has to be removed first -- lfs setstripe then
# creates the target itself as an empty file with the requested layout,
# and H5Fcreate's later H5F_ACC_TRUNC preserves that layout.
rm -f "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_FILE" || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,n_elem,write_setup_s,write_s,write_close_s,relaunch_s,analyze_setup_s,readback_s,compute_s,writeback_s,analyze_close_s,total_s,bad" > "$RESULTS"

./srun_hdf5_write.sh

# Unlike the PDC posthoc benchmark, there's no server to close and
# restart here -- HDF5 talks straight to the file, so the only relaunch
# cost between the two phases is plain Slurm srun-launch overhead, not
# something this script explicitly does, so there's nothing to time here
# (kept as an explicit 0 column so the two benchmarks' CSVs line up).
# H5Fclose itself, previously assumed free on the theory that data
# written via collective H5Dwrite is already durable, is NOT assumed
# here anymore -- hdf5_bench_write.c / hdf5_bench_posthoc_analyze.c now
# each measure their own H5Fclose() directly (write_close_s,
# analyze_close_s below), the same treatment PDC's close_server and
# ADIOS2's writer.Close() get, so no system's close cost goes
# unmeasured by default assumption.
relaunch_s=0

./srun_hdf5_posthoc_analyze.sh

# Both WRITE_LOG and ANALYZE_LOG now hold one line per timestep
# (N_TIMESTEPS=3 in hdf5_bench_write.c / hdf5_bench_posthoc_analyze.c), in
# matching step order, so pair them up line-by-line via two file
# descriptors and emit one combined "hdf5" row per timestep.
exec 3< "$WRITE_LOG"
exec 4< "$ANALYZE_LOG"
while IFS=, read -r w_mode w_step w_nranks w_nelem w_setup w_write w_close <&3 && \
      IFS=, read -r a_mode a_step a_nranks a_nelem a_setup a_readback a_compute a_writeback a_close a_bad <&4; do
  if [ "$w_setup" = "FAILED" ] || [ "$a_setup" = "FAILED" ]; then
    echo "hdf5,${w_step},${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" >> "$RESULTS"
  else
    # total_s deliberately excludes w_close/a_close -- they're carried as
    # their own columns (for direct visibility, per-phase) and folded in
    # exactly once, below, via avg_close_s -> total_with_close_s. Summing
    # them in here too would double-count them.
    total=$(awk "BEGIN{print $w_setup + $w_write + $relaunch_s + $a_setup + $a_readback + $a_compute + $a_writeback}")
    echo "hdf5,${w_step},${CLIENT_TOTAL_TASKS},${N_ELEM},${w_setup},${w_write},${w_close},${relaunch_s},${a_setup},${a_readback},${a_compute},${a_writeback},${a_close},${total},${a_bad}" >> "$RESULTS"
    # w_close/a_close are constant across every timestep row (one-time
    # H5Fclose per phase) -- capture once for the trailing avg_close_s
    # column below.
    final_w_close=$w_close
    final_a_close=$a_close
  fi
done
exec 3<&-
exec 4<&-

# No PDC server here to checkpoint on close, but H5Fclose cost is real
# and now measured (write_close_s + analyze_close_s above) -- sum the
# two per-phase closes into avg_close_s so this trailing column means
# the same thing ("real close/flush cost") across every mode's CSV and
# plot_totals.py's aggregate() (which reads this exact column name)
# folds it in identically instead of treating HDF5 as having none.
avg_close_s=$(awk "BEGIN{print ${final_w_close:-0} + ${final_a_close:-0}}")
awk -F, -v OFS=, -v avgclose="$avg_close_s" '
  NR==1 { print $0, "avg_close_s", "total_with_close_s"; next }
  NF==15 { print $0, avgclose, ($14 + avgclose); next }
  { print }
' "$RESULTS" > "${RESULTS}.tmp" && mv "${RESULTS}.tmp" "$RESULTS"


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
