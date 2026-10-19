#!/bin/bash
# Submits one compression_encryption_analysis.sbatch job per node count
# (1, 2, 4, 8, 16, 32, 64, 128), chained with --dependency=afterok.

cd "$(dirname "$0")"

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes compression_encryption_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --dependency=afterok:$prev_jid compression_encryption_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
