#!/bin/bash
# Submits one curl_eager_compress_analysis.sbatch job per node count (1,
# 2, 4, 8), chained with --dependency=afterok so they run one at a time
# and each gets its own results_curl_eager_compress_<jobid>.csv. Mirrors
# curl_eager_analysis_run.sh; COMPRESS is fixed at 1 inside the sbatch
# itself (not an environment toggle here) since this run script's whole
# purpose is producing the GPU-compressed variant as its own comparable
# dataset -- use curl_eager_analysis_run.sh for the uncompressed sweep.

cd "$(dirname "$0")"

source ../../common.sh
# SERVER_SWEEP is a space-separated list (default "2 4 8") -- allocate
# enough task slots/node for the LARGEST sweep step, since the sbatch
# script now runs the whole sweep within one job (see its own header
# comment); the loop inside it sets a smaller --ntasks-per-node per srun
# call for the smaller sweep steps.
SWEEP_MAX=$(printf "%s\\n" $SERVER_SWEEP | sort -n | tail -1)
NTASKS_PER_NODE=$((SWEEP_MAX + CLIENTS_PER_NODE))

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE curl_eager_compress_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE --dependency=afterok:$prev_jid curl_eager_compress_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
