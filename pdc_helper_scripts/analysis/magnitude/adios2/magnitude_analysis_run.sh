#!/bin/bash
# Submits one magnitude_analysis.sbatch job per node count (1, 2, 4, 8,
# 16, 32, 64, 128 -- 32 ranks/node, so up to 4096 ranks total), chained
# with --dependency=afterok so they run one at a time and each gets its
# own results_adios2_magnitude_<jobid>.csv. Mirrors
# pdc_helper_scripts/analysis_scripts/eager_pdc_run.sh's node range.

cd "$(dirname "$0")"

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes magnitude_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --dependency=afterok:$prev_jid magnitude_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
