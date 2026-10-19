#!/bin/bash
# PDC eager (DataFlyway) curl + vorticity-magnitude analysis, TRANSIENT
# curl variant: curl_x/y/z are declared transient in
# an_client/graphs/curl_vorticity_magnitude_transient.json, so the server
# computes them within the write and never persists them -- only u, v, w
# and vorticity_magnitude reach storage. Runs at 4 PDC servers/node only
# (SERVER_SWEEP is pinned to 4 below), 32 client ranks/node, one workload
# run per job. Output -- results_curl_eager_transient_<jobid>.csv, every
# srun log, and copies of the BIN_DIR logs -- lands in ./<MM_DD_YYYY>-<jobid>/.
#
# Submit directly, e.g.: sbatch --nodes=8 curl_eager_transient_analysis.sbatch
# (or sweep node counts with curl_eager_transient_analysis_run.sh).

#SBATCH --job-name=pdc-curl-eager-transient
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=40
#SBATCH --cpus-per-task=3
#SBATCH --account=m2621
#SBATCH --time=01:15:00
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --output=%x_%j.log

set -u
cd "$SLURM_SUBMIT_DIR"

# SERVER_SWEEP is pinned to 4 servers/node here (see below); CLIENTS_PER_NODE, NX/NY/NZ_PER_RANK: see
# ../../common.sh -- this file's own #SBATCH --ntasks-per-node above must
# still equal the LARGEST value in SERVER_SWEEP plus CLIENTS_PER_NODE
# (see that file's caveat).
source "$SLURM_SUBMIT_DIR/../../common.sh"
# Pinned: this variant is only run at 4 PDC servers per node.
export SERVER_SWEEP=4

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD/../../../../build/bin}
export PDC_DATA_LOC=${PDC_DATA_LOC:-$SCRATCH/pdc_curl_analysis_bench}
export PDC_TMPDIR=${PDC_TMPDIR:-$PDC_DATA_LOC}
mkdir -p "$PDC_DATA_LOC"

export LOG_TAG=curl_eager_transient
OUTDIR=$PWD/$(date +%m_%d_%Y)-${SLURM_JOB_ID}
mkdir -p "$OUTDIR"
export RESULTS=$OUTDIR/results_curl_eager_transient_${SLURM_JOB_ID}.csv

echo "servers_per_node,mode,step,n_ranks,nx,ny,nz_per_rank,compress,setup_s,write_s,confirm_read_s,total_s,bad,avg_close_s,total_with_close_s" > "$RESULTS"

for SERVERS_PER_NODE in $SERVER_SWEEP; do
  export SERVERS_PER_NODE
  export SERVER_TOTAL_TASKS=$((NUM_NODES * SERVERS_PER_NODE))

  # srun_server.sh clears $PDC_DATA_LOC itself before starting, so each
  # sweep step gets a fresh server with none of the previous step's data
  # left over.
  ./srun_server.sh
  STEP_RAW=$(mktemp "$OUTDIR/.step_raw.XXXXXX")
  RESULTS="$STEP_RAW" TRANSIENT=1 ./srun_client_curl_eager.sh
  # Relabel so this variant stays distinct from plain curl_eager runs in
  # any downstream consumer (same trick curl_eager_compress uses).
  sed -i 's/^curl_eager,/curl_eager_transient,/' "$STEP_RAW"
  ./srun_close_server.sh

  # Sanity-check the data volume that actually landed on disk against what
  # N_TIMESTEPS x NX x NY x NZ_PER_RANK x CLIENT_TOTAL_TASKS should produce:
  # u/v/w (float32) plus curl_x/y/z and vorticity_magnitude (float64), all
  # persisted once per timestep -- identical formula to
  # plot_curl_totals.py's aggregate(). PDC object data lives under
  # $PDC_DATA_LOC/pdc_data (see pdc_server_data.c's storage_location),
  # which srun_server.sh just wiped for this sweep step, so this only
  # measures what this step itself wrote. `du -sb` (apparent size, not
  # block-rounded) should match almost exactly for uncompressed data --
  # this catches truncated writes, a stale/reused data directory, or a
  # region silently not flushed, none of which `bad=0` alone would catch
  # (bad only checks values read back through PDC, not what actually
  # reached storage).
  N_TIMESTEPS=3   # must track bench_curl_eager.c's #define N_TIMESTEPS
  BYTES_PER_ELEM=$((4 * 3 + 8 * 1))   # transient: only u,v,w float32 + vorticity_magnitude float64 persist
  EXPECTED_BYTES=$((CLIENT_TOTAL_TASKS * NX * NY * NZ_PER_RANK * BYTES_PER_ELEM * N_TIMESTEPS))
  ACTUAL_BYTES=$(du -sb "$PDC_DATA_LOC/pdc_data" 2>/dev/null | awk '{print $1}')
  ACTUAL_BYTES=${ACTUAL_BYTES:-0}
  if [ "$COMPRESS" = "1" ]; then
    echo "[servers_per_node=${SERVERS_PER_NODE}] Data size check (COMPRESS=1, vorticity_magnitude compressed -- actual < expected uncompressed is correct): expected=${EXPECTED_BYTES} bytes, actual=${ACTUAL_BYTES} bytes"
  else
    echo "[servers_per_node=${SERVERS_PER_NODE}] Data size check: expected=${EXPECTED_BYTES} bytes, actual=${ACTUAL_BYTES} bytes"
    awk -v e="$EXPECTED_BYTES" -v a="$ACTUAL_BYTES" 'BEGIN {
      if (e == 0) { print "WARNING: expected byte count is 0, skipping ratio check"; exit }
      ratio = a / e
      if (ratio < 0.95 || ratio > 1.05)
        printf "WARNING: actual/expected data size ratio %.3f is outside [0.95, 1.05] -- possible incomplete write, stale data directory, or an unflushed region\n", ratio
      else
        printf "OK: actual/expected data size ratio %.3f within tolerance\n", ratio
    }'
  fi

  # close_server.c logs "total close time = X" per server rank -- that's
  # the checkpoint that actually flushes PDC's region cache (up to 64GB by
  # default; see pdc_server_region_cache.c) to the storage backend, a real
  # durability cost with no HDF5 equivalent. Average it across server
  # ranks and append avg_close_s/total_with_close_s so total_s alone
  # doesn't make eager look artificially cheap for data that fit in cache
  # and was never actually flushed during write_s.
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
