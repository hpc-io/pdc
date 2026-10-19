#!/bin/bash
# Parallel-HDF5 curl + vorticity-magnitude analysis baseline -- single
# node-count step. No PDC server involved; see hdf5_analysis_test/. Two
# genuinely separate srun steps (write u,v,w; then read them back and
# compute curl then magnitude in one pass), mirroring the PDC posthoc
# benchmark's phase structure -- see hdf5_bench_curl_write.c / _analyze.c.
# Writes one CSV row to results_curl_hdf5_<jobid>.csv.
#
# Submit one of these per node count via curl_hdf5_analysis_run.sh, or
# directly, e.g.: sbatch --nodes=4 curl_hdf5_analysis.sbatch
# Override NX/NY/NZ_PER_RANK at submit time, e.g.:
#   NZ_PER_RANK=128 sbatch --nodes=4 curl_hdf5_analysis.sbatch
#
# Needs the same parallel-HDF5 module that built hdf5_analysis_test/ (see
# hdf5_analysis_test/README.md) loaded here too -- the compute-node
# environment doesn't inherit whatever was loaded in the login shell at
# build time, so the module load below is required even though the
# binaries were already built.

#SBATCH --job-name=hdf5-curl-posthoc
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

# CLIENTS_PER_NODE, NX/NY/NZ_PER_RANK: see ../common.sh (no PDC server
# here, so SERVERS_PER_NODE from that file is unused). This file's own
# #SBATCH --ntasks-per-node above must still equal CLIENTS_PER_NODE.
source "$SLURM_SUBMIT_DIR/../../common.sh"

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD}
export OUT_DIR=${OUT_DIR:-$SCRATCH/hdf5_curl_analysis_bench}
mkdir -p "$OUT_DIR"

export LOG_TAG=curl_hdf5
export RESULTS=$PWD/results_curl_hdf5_${SLURM_JOB_ID}.csv
export OUT_FILE=${OUT_DIR}/hdf5_bench_curl_${NUM_NODES}.h5
export WRITE_LOG=$PWD/write_${LOG_TAG}_${SLURM_JOB_ID}.csv
export ANALYZE_LOG=$PWD/analyze_${LOG_TAG}_${SLURM_JOB_ID}.csv

# Lustre stripe size == exactly what one rank writes per collective write
# (NX*NY*NZ_PER_RANK float32 elements), rounded up to the required 64 KiB
# stripe granularity, striped across all 128 OSTs -- see
# analysis_scripts/hdf5_analysis.sbatch's comment for the full reasoning.
STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((NX * NY * NZ_PER_RANK * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
echo "Lustre striping: count=${STRIPE_COUNT} size=${STRIPE_SIZE_BYTES} bytes ($((STRIPE_SIZE_BYTES / 1048576)) MiB/rank), target=${OUT_FILE}"

rm -f "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_FILE" || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,nx,ny,nz_per_rank,write_setup_s,write_s,analyze_setup_s,readback_s,curl_compute_s,curl_writeback_s,magnitude_compute_s,magnitude_writeback_s,total_s,bad" > "$RESULTS"

./srun_hdf5_curl_write.sh
./srun_hdf5_curl_analyze.sh

# Sanity-check the data volume that actually landed in the HDF5 file
# against what N_TIMESTEPS x NX x NY x NZ_PER_RANK x CLIENT_TOTAL_TASKS
# should produce: u/v/w (float32, from the write phase) plus curl_x/y/z
# and vorticity_magnitude (float64, from the analyze phase), all
# persisted once per timestep -- same formula as
# plot_curl_totals.py's aggregate(). Unlike the PDC-backed sbatch
# scripts, this reads the actual file size directly (no server-side
# cache to worry about -- H5Dwrite here goes straight through MPI-IO),
# with a slightly wider upper tolerance for HDF5's own per-chunk/dataset
# metadata overhead.
N_TIMESTEPS=3   # must track hdf5_bench_curl_write.c / hdf5_bench_curl_analyze.c's #define N_TIMESTEPS
BYTES_PER_ELEM=$((4 * 3 + 8 * 4))   # u,v,w float32 + curl_x/y/z,vorticity_magnitude float64
EXPECTED_BYTES=$((CLIENT_TOTAL_TASKS * NX * NY * NZ_PER_RANK * BYTES_PER_ELEM * N_TIMESTEPS))
ACTUAL_BYTES=$(stat -c%s "$OUT_FILE" 2>/dev/null || echo 0)
echo "Data size check: expected=${EXPECTED_BYTES} bytes, actual=${ACTUAL_BYTES} bytes (${OUT_FILE})"
awk -v e="$EXPECTED_BYTES" -v a="$ACTUAL_BYTES" 'BEGIN {
  if (e == 0) { print "WARNING: expected byte count is 0, skipping ratio check"; exit }
  ratio = a / e
  if (ratio < 0.95 || ratio > 1.15)
    printf "WARNING: actual/expected data size ratio %.3f is outside [0.95, 1.15] -- possible incomplete write or stale file\n", ratio
  else
    printf "OK: actual/expected data size ratio %.3f within tolerance\n", ratio
}'

# Both WRITE_LOG and ANALYZE_LOG now hold one line per timestep
# (N_TIMESTEPS=3 in hdf5_bench_curl_write.c / hdf5_bench_curl_analyze.c),
# in matching step order, so pair them up line-by-line via two file
# descriptors and emit one combined "curl_hdf5" row per timestep.
exec 3< "$WRITE_LOG"
exec 4< "$ANALYZE_LOG"
while IFS=, read -r w_mode w_step w_nranks w_nx w_ny w_nz w_setup w_write <&3 && \
      IFS=, read -r a_mode a_step a_nranks a_nx a_ny a_nz a_setup a_readback a_curl_compute a_curl_writeback a_mag_compute a_mag_writeback a_total a_bad <&4; do
  if [ "$w_setup" = "FAILED" ] || [ "$a_setup" = "FAILED" ]; then
    echo "curl_hdf5,${w_step},${CLIENT_TOTAL_TASKS},${NX},${NY},${NZ_PER_RANK},FAILED" >> "$RESULTS"
  else
    total=$(awk "BEGIN{print $w_setup + $w_write + $a_setup + $a_readback + $a_curl_compute + $a_curl_writeback + $a_mag_compute + $a_mag_writeback}")
    echo "curl_hdf5,${w_step},${CLIENT_TOTAL_TASKS},${NX},${NY},${NZ_PER_RANK},${w_setup},${w_write},${a_setup},${a_readback},${a_curl_compute},${a_curl_writeback},${a_mag_compute},${a_mag_writeback},${total},${a_bad}" >> "$RESULTS"
  fi
done
exec 3<&-
exec 4<&-

# No PDC server here, so no checkpoint/flush cost to measure -- avg_close_s
# is always 0, keeping this schema directly comparable to
# results_curl_eager_*.csv / results_curl_posthoc_*.csv's own
# avg_close_s/total_with_close_s columns (total_with_close_s == total_s
# here since avg_close_s is 0).
awk -F, -v OFS=, '
  NR==1 { print $0, "avg_close_s", "total_with_close_s"; next }
  NF==16 { print $0, "0", $15; next }
  { print }
' "$RESULTS" > "${RESULTS}.tmp" && mv "${RESULTS}.tmp" "$RESULTS"


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
