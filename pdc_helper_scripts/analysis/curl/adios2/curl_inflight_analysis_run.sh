#!/bin/bash
# Submits one curl_inflight_analysis.sbatch job per node count (1, 2, 4,
# 8, 16, 32, 64, 128), chained with --dependency=afterok. Mirrors
# curl_analysis_run.sh's node range for direct comparison.

cd "$(dirname "$0")"

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes curl_inflight_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --dependency=afterok:$prev_jid curl_inflight_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
