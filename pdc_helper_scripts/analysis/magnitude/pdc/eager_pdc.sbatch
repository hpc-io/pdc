#!/bin/bash
# PDC DataFlyway (eager, in-flight) magnitude analysis -- one job runs the
# full SERVER_SWEEP (2/4/8 servers/node by default, see ../../common.sh)
# as three back-to-back workload runs, PDC's data directory wiped between
# them by srun_server.sh's own startup. 32 client ranks/node (see
# srun_client_eager.sh). All of this job's output -- the combined
# results_eager_<jobid>.csv (one servers_per_node-tagged row set per
# sweep step), every srun log, and copies of the per-step BIN_DIR logs
# (server/client/close_server -- copied out because their filenames are
# keyed only by node count, so the next sweep step would otherwise
# silently overwrite them) -- lands in ./<MM_DD_YYYY>-<jobid>/.
#
# Submit directly, e.g.: sbatch --nodes=8 eager_pdc.sbatch
# Override N_ELEM or the sweep itself at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=8 eager_pdc.sbatch
#   SERVER_SWEEP="2 8" sbatch --nodes=8 eager_pdc.sbatch

#SBATCH --job-name=pdc-eager-magnitude
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

# SERVER_SWEEP, CLIENTS_PER_NODE, N_ELEM: see ../../common.sh -- this
# file's own #SBATCH --ntasks-per-node above must still equal the LARGEST
# value in SERVER_SWEEP plus CLIENTS_PER_NODE (see that file's caveat).
source "$SLURM_SUBMIT_DIR/../../common.sh"

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD/../../../../build/bin}
export PDC_DATA_LOC=${PDC_DATA_LOC:-$SCRATCH/pdc_analysis_bench}
export PDC_TMPDIR=${PDC_TMPDIR:-$PDC_DATA_LOC}
mkdir -p "$PDC_DATA_LOC"

export LOG_TAG=eager
OUTDIR=$PWD/$(date +%m_%d_%Y)-${SLURM_JOB_ID}
mkdir -p "$OUTDIR"
export RESULTS=$OUTDIR/results_eager_${SLURM_JOB_ID}.csv

echo "servers_per_node,mode,step,n_ranks,n_elem,setup_s,write_s,readback_s,compute_s,writeback_s,confirm_read_s,step_total_s,bad,avg_close_s,total_with_close_s" > "$RESULTS"

for SERVERS_PER_NODE in $SERVER_SWEEP; do
  export SERVERS_PER_NODE
  export SERVER_TOTAL_TASKS=$((NUM_NODES * SERVERS_PER_NODE))

  # srun_server.sh clears $PDC_DATA_LOC itself before starting, so each
  # sweep step gets a fresh server with none of the previous step's data
  # left over.
  ./srun_server.sh
  STEP_RAW=$(mktemp "$OUTDIR/.step_raw.XXXXXX")
  RESULTS="$STEP_RAW" ./srun_client_eager.sh
  ./srun_close_server.sh

  # srun_close_server.sh's log holds one "total close time = X" line per
  # server rank (printed by close_server.c, the PDC_Client_close_all_server
  # RPC that checkpoints every server's in-memory metadata to disk before
  # exiting). That's a real, PDC-specific durability cost with no HDF5
  # equivalent -- H5Fclose in hdf5_bench_write.c isn't even separately
  # timed, since closing a file backed by already-durable collective writes
  # is comparatively free. Without folding this in, step_total_s makes
  # eager/lazy look cheaper than they really are next to the HDF5 baseline,
  # so average it across server ranks and append it as its own column plus
  # a close-inclusive total, rather than leaving it buried in a log file
  # nothing else reads.
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
