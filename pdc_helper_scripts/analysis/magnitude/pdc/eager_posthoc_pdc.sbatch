#!/bin/bash
# PDC eager_posthoc (DataFlyway's eager graph engine, attached and
# triggered in a posthoc shape) magnitude analysis -- single node-count
# step. 2 data servers/node, 32 client ranks/node (see srun_server.sh /
# srun_client_posthoc_write.sh / srun_client_posthoc_analyze_eager.sh),
# writing one CSV row to results_eager_posthoc_<jobid>.csv.
#
# Phase 1 (bench_write_components, unchanged from plain posthoc) writes
# vx/vy/vz as plain PDC objects -- no graph attached, no DataFlyway
# involvement at all -- then every client rank fully exits. Phase 2
# (bench_posthoc_analyze_eager, new) runs in a completely separate
# process, after a real server close+restart, and attaches the eager
# vector_magnitude graph to those already-durable vx/vy/vz objects plus a
# freshly created magnitude object; its one read of magnitude is what
# triggers the server to compute+persist it, via the same engine
# bench_magnitude.c's eager mode uses, but triggered by a read instead of
# a write (see bench_posthoc_analyze_eager.c's header comment for why that
# works). This measures attaching an analysis pipeline to data after the
# fact, in a later session that had no part in writing it -- a real,
# different use case from both eager (attached at write time, same
# session) and plain posthoc (no framework at all, hand-rolled client
# compute).
#
# One job runs the full SERVER_SWEEP (2/4/8 servers/node by default, see
# ../../common.sh) as three back-to-back workload runs, PDC's data
# directory wiped between them by srun_server.sh's own startup. All of
# this job's output -- the combined results_eager_posthoc_<jobid>.csv
# (one servers_per_node-tagged row set per sweep step), every srun log,
# and copies of the per-step BIN_DIR logs (copied out because their
# filenames are keyed only by node count, so the next sweep step would
# otherwise silently overwrite them) -- lands in
# ./<MM_DD_YYYY>-<jobid>/.
#
# Submit directly, e.g.: sbatch --nodes=8 eager_posthoc_pdc.sbatch
# Override N_ELEM or the sweep itself at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=8 eager_posthoc_pdc.sbatch
#   SERVER_SWEEP="2 8" sbatch --nodes=8 eager_posthoc_pdc.sbatch

#SBATCH --job-name=pdc-eager-posthoc-magnitude
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

export LOG_TAG=eager_posthoc
OUTDIR=$PWD/$(date +%m_%d_%Y)-${SLURM_JOB_ID}
mkdir -p "$OUTDIR"
export RESULTS=$OUTDIR/results_eager_posthoc_${SLURM_JOB_ID}.csv
export WRITE_LOG=$OUTDIR/write_${LOG_TAG}_${SLURM_JOB_ID}.csv
export ANALYZE_LOG=$OUTDIR/analyze_${LOG_TAG}_${SLURM_JOB_ID}.csv

echo "servers_per_node,mode,step,n_ranks,n_elem,write_setup_s,write_s,relaunch_s,analyze_setup_s,readback_s,compute_s,writeback_s,total_s,bad,avg_close_s,total_with_close_s" > "$RESULTS"

for SERVERS_PER_NODE in $SERVER_SWEEP; do
  export SERVERS_PER_NODE
  export SERVER_TOTAL_TASKS=$((NUM_NODES * SERVERS_PER_NODE))

  # Phase 1: write vx/vy/vz as plain PDC objects (bench_write_components
  # is shared, byte-for-byte, with plain posthoc -- see its header
  # comment), then let every client rank fully exit. srun_server.sh clears
  # $PDC_DATA_LOC itself before starting, so each sweep step gets a fresh
  # server with none of the previous step's data left over.
  ./srun_server.sh
  ./srun_client_posthoc_write.sh

  # Relaunch: close the server (checkpoints its metadata, including every
  # analysis binding's materialized flag -- see pdc_an_server.c), then
  # restart it with the `restart` argument so it reloads that checkpoint.
  # The eager graph itself doesn't exist yet at this point -- phase 2
  # creates it fresh -- so this relaunch is purely about vx/vy/vz's
  # durability, exactly like plain posthoc's.
  relaunch_t0=$(date +%s.%N)
  ./srun_close_server.sh
  ./srun_server_restart.sh
  relaunch_t1=$(date +%s.%N)
  relaunch_s=$(awk "BEGIN{print $relaunch_t1 - $relaunch_t0}")

  # Phase 2: a fresh client attaches the eager graph to the already-durable
  # vx/vy/vz plus a new magnitude object, and reads magnitude -- that read
  # triggers the server-side computation (see
  # bench_posthoc_analyze_eager.c).
  ./srun_client_posthoc_analyze_eager.sh
  ./srun_close_server.sh

  # Both WRITE_LOG and ANALYZE_LOG now hold one line per timestep
  # (N_TIMESTEPS=3), in matching step order, so pair them up line-by-line
  # via two file descriptors and emit one combined "eager_posthoc" row per
  # timestep into this step's own raw file -- same shape as
  # posthoc_pdc.sbatch's combiner, except ANALYZE_LOG's
  # compute_s/writeback_s fields are always 0 here (the server does both,
  # inseparably, inside readback_s -- see
  # bench_posthoc_analyze_eager.c's header comment).
  STEP_RAW=$(mktemp "$OUTDIR/.step_raw.XXXXXX")
  exec 3< "$WRITE_LOG"
  exec 4< "$ANALYZE_LOG"
  while IFS=, read -r w_mode w_step w_nranks w_nelem w_setup w_write <&3 && \
        IFS=, read -r a_mode a_step a_nranks a_nelem a_setup a_readback a_compute a_writeback a_total a_bad <&4; do
    if [ "$w_setup" = "FAILED" ] || [ "$a_setup" = "FAILED" ]; then
      echo "eager_posthoc,${w_step},${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" >> "$STEP_RAW"
    else
      total=$(awk "BEGIN{print $w_setup + $w_write + $relaunch_s + $a_setup + $a_readback + $a_compute + $a_writeback}")
      echo "eager_posthoc,${w_step},${CLIENT_TOTAL_TASKS},${N_ELEM},${w_setup},${w_write},${relaunch_s},${a_setup},${a_readback},${a_compute},${a_writeback},${total},${a_bad}" >> "$STEP_RAW"
    fi
  done
  exec 3<&-
  exec 4<&-

  # Average per-rank server checkpoint/shutdown cost from this step's
  # final close_server log -- see the identical block in posthoc_pdc.sbatch
  # for why this matters for an HDF5-comparable total.
  CLOSE_LOG="$BIN_DIR/close_server_${LOG_TAG}_${NUM_NODES}.log"
  avg_close_s=$(grep 'total close time' "$CLOSE_LOG" 2>/dev/null | \
    sed -n 's/.*total close time = \([0-9.]*\).*/\1/p' | \
    awk '{sum+=$1; n++} END{if(n>0) printf "%.6f", sum/n; else print "0"}')
  # Fold in close time and prepend servers_per_node, then append this
  # step's (headerless) rows to the shared, multi-step $RESULTS.
  awk -F, -v OFS=, -v avgclose="$avg_close_s" -v srv="$SERVERS_PER_NODE" '
    NF==13 { print srv, $0, avgclose, ($12 + avgclose); next }
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
