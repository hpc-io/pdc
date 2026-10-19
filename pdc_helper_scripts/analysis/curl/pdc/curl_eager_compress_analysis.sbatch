#!/bin/bash
# PDC eager (DataFlyway) curl + vorticity-magnitude analysis, GPU-ZFP-
# COMPRESSED vorticity_magnitude variant -- single node-count step.
# Mechanically identical to curl_eager_analysis.sbatch (one continuous
# client session; writing u,v,w with the graph attached transparently
# triggers server-side curl + vector_magnitude in the write path), except
# vorticity_magnitude is additionally composed with the existing GPU ZFP
# compression transform before that write -- see
# src/tests/analysis/bench_curl_eager.c and curl_analysis_scripts/
# README.md's "Compression" section.
#
# Kept as its own dedicated test type (own sbatch, own
# results_curl_eager_compress_<jobid>.csv, own "curl_eager_compress" mode
# label) instead of only an env-var toggle on plain eager, so it shows up
# in plot_curl_totals.py as its own directly comparable workload rather
# than being folded into curl_eager's numbers via the CSV's own compress
# column.
#
# One job runs the full SERVER_SWEEP (2/4/8 servers/node by default, see
# ../../common.sh) as three back-to-back workload runs, PDC's data
# directory wiped between them by srun_server.sh's own startup. All of
# this job's output -- the combined
# results_curl_eager_compress_<jobid>.csv (one servers_per_node-tagged
# row set per sweep step), every srun log, and copies of the per-step
# BIN_DIR logs (copied out because their filenames are keyed only by node
# count, so the next sweep step would otherwise silently overwrite them)
# -- lands in ./<MM_DD_YYYY>-<jobid>/.
#
# Submit directly, e.g.: sbatch --nodes=8 curl_eager_compress_analysis.sbatch
# Override NX/NY/NZ_PER_RANK or the sweep itself at submit time, e.g.:
#   NZ_PER_RANK=128 sbatch --nodes=8 curl_eager_compress_analysis.sbatch
#   SERVER_SWEEP="2 8" sbatch --nodes=8 curl_eager_compress_analysis.sbatch

#SBATCH --job-name=pdc-curl-eager-compress
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=40
#SBATCH --cpus-per-task=3
#SBATCH --account=m2621
#SBATCH --time=01:15:00
#SBATCH --constraint=gpu
#SBATCH --gpus-per-node=4
#SBATCH --qos=regular
#SBATCH --output=%x_%j.log

set -u
cd "$SLURM_SUBMIT_DIR"

# SERVER_SWEEP, CLIENTS_PER_NODE, NX/NY/NZ_PER_RANK: see ../../common.sh
# -- this file's own #SBATCH --ntasks-per-node above must still equal the
# LARGEST value in SERVER_SWEEP plus CLIENTS_PER_NODE (see that file's
# caveat). COMPRESS is forced to 1 below regardless of common.sh's
# default -- that's the whole point of this dedicated variant (see file
# header comment).
source "$SLURM_SUBMIT_DIR/../../common.sh"
export COMPRESS=1

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD/../../../../build/bin}
export PDC_DATA_LOC=${PDC_DATA_LOC:-$SCRATCH/pdc_curl_analysis_bench}
export PDC_TMPDIR=${PDC_TMPDIR:-$PDC_DATA_LOC}
mkdir -p "$PDC_DATA_LOC"

export LOG_TAG=curl_eager_compress
OUTDIR=$PWD/$(date +%m_%d_%Y)-${SLURM_JOB_ID}
mkdir -p "$OUTDIR"
export RESULTS=$OUTDIR/results_curl_eager_compress_${SLURM_JOB_ID}.csv

echo "servers_per_node,mode,step,n_ranks,nx,ny,nz_per_rank,compress,setup_s,write_s,confirm_read_s,total_s,bad,avg_close_s,total_with_close_s" > "$RESULTS"

for SERVERS_PER_NODE in $SERVER_SWEEP; do
  export SERVERS_PER_NODE
  export SERVER_TOTAL_TASKS=$((NUM_NODES * SERVERS_PER_NODE))

  # srun_server.sh clears $PDC_DATA_LOC itself before starting, so each
  # sweep step gets a fresh server with none of the previous step's data
  # left over.
  ./srun_server.sh
  STEP_RAW=$(mktemp "$OUTDIR/.step_raw.XXXXXX")
  RESULTS="$STEP_RAW" ./srun_client_curl_eager.sh
  ./srun_close_server.sh

  # Sanity-check the data volume that actually landed on disk against what
  # N_TIMESTEPS x NX x NY x NZ_PER_RANK x CLIENT_TOTAL_TASKS should produce
  # uncompressed (u/v/w float32 plus curl_x/y/z and vorticity_magnitude
  # float64 -- same formula as plot_curl_totals.py's aggregate()). With
  # COMPRESS=1, vorticity_magnitude is GPU-ZFP-compressed, so actual <
  # expected is the correct, expected outcome here -- this just reports
  # the numbers (and the resulting ratio, informally the achieved
  # compression's effect on total footprint) rather than asserting a
  # tolerance band the way the uncompressed sbatch scripts do.
  N_TIMESTEPS=3   # must track bench_curl_eager.c's #define N_TIMESTEPS
  BYTES_PER_ELEM=$((4 * 3 + 8 * 4))   # u,v,w float32 + curl_x/y/z,vorticity_magnitude float64
  EXPECTED_BYTES=$((CLIENT_TOTAL_TASKS * NX * NY * NZ_PER_RANK * BYTES_PER_ELEM * N_TIMESTEPS))
  ACTUAL_BYTES=$(du -sb "$PDC_DATA_LOC/pdc_data" 2>/dev/null | awk '{print $1}')
  ACTUAL_BYTES=${ACTUAL_BYTES:-0}
  echo "[servers_per_node=${SERVERS_PER_NODE}] Data size check (COMPRESS=1, vorticity_magnitude compressed -- actual < expected uncompressed is correct): expected=${EXPECTED_BYTES} bytes, actual=${ACTUAL_BYTES} bytes"

  # srun_client_curl_eager.sh appends bench_curl_eager's own "curl_eager,"
  # line verbatim (same binary as plain eager, distinguished only by its
  # compress column) -- relabel the mode field here so this test type
  # stays distinct even in a results directory that also has plain
  # (uncompressed) curl_eager runs, instead of relying on every downstream
  # consumer to remember to check the compress column.
  sed -i 's/^curl_eager,/curl_eager_compress,/' "$STEP_RAW"

  # close_server.c logs "total close time = X" per server rank -- that's
  # the checkpoint that actually flushes PDC's region cache to the storage
  # backend, a real durability cost with no HDF5 equivalent. Average it
  # across server ranks and append avg_close_s/total_with_close_s so
  # total_s alone doesn't make eager look artificially cheap for data that
  # fit in cache and was never actually flushed during write_s.
  CLOSE_LOG="$BIN_DIR/close_server_${LOG_TAG}_${NUM_NODES}.log"
  avg_close_s=$(grep 'total close time' "$CLOSE_LOG" 2>/dev/null | \
    sed -n 's/.*total close time = \([0-9.]*\).*/\1/p' | \
    awk '{sum+=$1; n++} END{if(n>0) printf "%.6f", sum/n; else print "0"}')
  # Fold in close time and prepend servers_per_node, then append this
  # step's (headerless) rows to the shared, multi-step $RESULTS.
  awk -F, -v OFS=, -v avgclose="$avg_close_s" -v srv="$SERVERS_PER_NODE" '
    NF==12 { print srv, $0, avgclose, ($11 + avgclose); next }
    { print srv, $0 }
  ' "$STEP_RAW" >> "$RESULTS"
  rm -f "$STEP_RAW"

  # Copy this step's BIN_DIR logs out before the next sweep step
  # overwrites them (server/client/close_server filenames are keyed only
  # by $NUM_NODES, not servers-per-node -- see srun_*.sh) into this job's
  # own output folder, tagged by server count.
  for f in "$BIN_DIR"/*"${LOG_TAG}"*"_${NUM_NODES}".log "$BIN_DIR"/*"${LOG_TAG}"*"_${NUM_NODES}".err; do
    [ -e "$f" ] || continue
    cp "$f" "$OUTDIR/${SERVERS_PER_NODE}servers_$(basename "$f")"
  done
done

# Move this job's own top-level Slurm stdout/stderr log into OUTDIR as the
# very last step -- rename() doesn't invalidate Slurm's already-open file
# descriptor, so anything written after this line (just the final echo
# below) still lands in the new location.
mv "$PWD/${SLURM_JOB_NAME}_${SLURM_JOB_ID}.log" "$OUTDIR/" 2>/dev/null || true

echo "Done. Results in $RESULTS"
