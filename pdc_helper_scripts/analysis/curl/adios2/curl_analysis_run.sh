#!/bin/bash
# Submits one curl_analysis.sbatch job per node count (1, 2, 4, 8, 16,
# 32, 64, 128), chained with --dependency=afterok. Mirrors
# pdc_helper_scripts/curl_analysis_scripts/curl_eager_analysis_run.sh's
# node range.

cd "$(dirname "$0")"

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes curl_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --dependency=afterok:$prev_jid curl_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
