#!/bin/bash
# Submits one vpicio_bdcats.sbatch job per node count (1, 2, 4, 8, 16,
# 32, 64, 128), chained with --dependency=afterok so they run one at a
# time and each gets its own results_adios2_vpicio_<jobid>.csv /
# results_adios2_bdcats_<jobid>.csv. Mirrors
# pdc_helper_scripts/vpicio_scripts/*_run.sh.

cd "$(dirname "$0")"

source ./common.sh
NTASKS_PER_NODE=$CLIENTS_PER_NODE

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE vpicio_bdcats.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE --dependency=afterok:$prev_jid vpicio_bdcats.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
