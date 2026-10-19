#!/bin/bash
# VPIC-IO with PDC's TF framework's GPU ZFP compression transform
# (tf_client/graphs/zfp_gpu.json, "zfp_gpu" in vpicio.c's
# transformation_str) -- one job runs the full SERVER_SWEEP (2/4/8
# servers/node by default, see ./common.sh) as three back-to-back
# workload runs, PDC's data directory wiped between them by
# srun_server_vpicio.sh's own startup. All of this job's output -- the
# combined results_zfp_compress_gpu_<jobid>.csv (one
# servers_per_node-tagged row set per sweep step), every srun log, and
# copies of the per-step BIN_DIR logs (copied out because their filenames
# are keyed only by node count, so the next sweep step would otherwise
# silently overwrite them) -- lands in ./<MM_DD_YYYY>-<jobid>/.
#
# Writer (vpicio) and a genuinely separate verification process
# (vpicio_verify, run as its own srun step after the writer fully exits)
# both run; only the writer's timing is folded into the results CSV --
# see vpicio.c's and vpicio_verify.c's own header comments for why
# verification is deliberately kept out of the workload-time
# measurement.
#
# Submit directly, e.g.: sbatch --nodes=8 zfp_compress_gpu.sbatch
# Override sizing or the sweep itself at submit time via ./common.sh's
# variables, e.g.:
#   NPARTICLES=16777216 sbatch --nodes=8 zfp_compress_gpu.sbatch
#   SERVER_SWEEP="2 8" sbatch --nodes=8 zfp_compress_gpu.sbatch

#SBATCH --job-name=vpicio-zfp-compress-gpu
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

# SERVER_SWEEP, CLIENTS_PER_NODE, NPARTICLES, STEPS, SLEEPTIME: see
# ./common.sh -- this file's own #SBATCH --ntasks-per-node above must
# still equal the LARGEST value in SERVER_SWEEP plus CLIENTS_PER_NODE
# (see that file's caveat).
source "$SLURM_SUBMIT_DIR/common.sh"
export TRANSFORM=zfp_gpu

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD/../../../build/bin}
export PDC_DATA_LOC=${PDC_DATA_LOC:-$SCRATCH/pdc_vpicio_bench}
export PDC_TMPDIR=${PDC_TMPDIR:-$PDC_DATA_LOC}
mkdir -p "$PDC_DATA_LOC"

export LOG_TAG=zfp_compress_gpu
OUTDIR=$PWD/$(date +%m_%d_%Y)-${SLURM_JOB_ID}
mkdir -p "$OUTDIR"
export RESULTS=$OUTDIR/results_zfp_compress_gpu_${SLURM_JOB_ID}.csv

echo "servers_per_node,mode,step,n_ranks,transform,setup_s,write_s,step_total_s,avg_close_s,total_with_close_s,last_step_write_s" > "$RESULTS"

for SERVERS_PER_NODE in $SERVER_SWEEP; do
  export SERVERS_PER_NODE
  export SERVER_TOTAL_TASKS=$((NUM_NODES * SERVERS_PER_NODE))

  # srun_server_vpicio.sh clears $PDC_DATA_LOC itself before starting, so
  # each sweep step gets a fresh server with none of the previous step's
  # data left over.
  ./srun_server_vpicio.sh
  ./srun_client_vpicio.sh
  ./srun_client_vpicio_verify.sh
  ./srun_close_server_vpicio.sh

  STEP_RAW=$(mktemp "$OUTDIR/.step_raw.XXXXXX")
  grep "^vpicio," "$BIN_DIR/client_${LOG_TAG}_${NUM_NODES}.log" >> "$STEP_RAW" || true

  # See srun_close_server_vpicio.sh's own header comment -- same
  # avg_close_s / total_with_close_s convention as analysis/. (Both srun
  # helpers pushd into $BIN_DIR before writing their logs, so read them
  # back from there too.)
  CLOSE_LOG="$BIN_DIR/close_server_${LOG_TAG}_${NUM_NODES}.log"
  avg_close_s=$(grep 'total close time' "$CLOSE_LOG" 2>/dev/null | \
    sed -n 's/.*total close time = \([0-9.]*\).*/\1/p' | \
    awk '{sum+=$1; n++} END{if(n>0) printf "%.6f", sum/n; else print "0"}')

  # The last of STEPS timesteps is VPIC-IO's real "outlier" I/O phase --
  # it must run to completion before the job can end, so it can never
  # overlap with a following step's compute the way every earlier step
  # can (see evaluation.tex's own "the outlier phase for VPIC-IO" framing
  # and vpicio.c's own SLEEPTIME comment). Report its write_s as its own
  # column, repeated on every row of this step (same one-time-per-job
  # convention as avg_close_s), rather than leaving callers to rediscover
  # "the last step" by re-deriving max(step) themselves.
  last_step_write_s=$(tail -1 "$STEP_RAW" 2>/dev/null | awk -F, '{print $6}')

  # Fold in close time and prepend servers_per_node, then append this
  # step's (headerless) rows to the shared, multi-step $RESULTS.
  awk -F, -v OFS=, -v avgclose="$avg_close_s" -v srv="$SERVERS_PER_NODE" -v laststep="$last_step_write_s" '
    NF==7 { print srv, $0, avgclose, ($7 + avgclose), laststep; next }
    { print srv, $0, "", "", laststep }
  ' "$STEP_RAW" >> "$RESULTS"
  rm -f "$STEP_RAW"

  # Copy this step's BIN_DIR logs out before the next sweep step
  # overwrites them (server/client/verify/close_server filenames are keyed
  # only by $NUM_NODES, not servers-per-node -- see srun_*.sh) into this
  # job's own output folder, tagged by server count.
  for f in "$BIN_DIR"/*"${LOG_TAG}"*"_${NUM_NODES}".log "$BIN_DIR"/*"${LOG_TAG}"*"_${NUM_NODES}".err; do
    [ -e "$f" ] || continue
    cp "$f" "$OUTDIR/${SERVERS_PER_NODE}servers_$(basename "$f")"
  done
done

# Move this job's own top-level Slurm stdout/stderr log into OUTDIR as the
# very last step -- rename() doesn't invalidate Slurm's already-open file
# descriptor, so anything written after this line (just the final echoes
# below) still lands in the new location.
mv "$PWD/${SLURM_JOB_NAME}_${SLURM_JOB_ID}.log" "$OUTDIR/" 2>/dev/null || true

echo "Done. Results in $RESULTS"
echo "Verification logs (not part of workload timing): ${OUTDIR}/*servers_verify_${LOG_TAG}_${NUM_NODES}.log"
