#!/bin/bash
# Submits one compression_analysis.sbatch job per node count (1, 2, 4,
# 8, 16, 32, 64, 128), chained with --dependency=afterok. Mirrors
# pdc_helper_scripts/vpicio_scripts/vpicio_scale_run.sh's node range.

cd "$(dirname "$0")"

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes compression_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --dependency=afterok:$prev_jid compression_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
