#!/bin/bash
# Submits one zfp_compress_cpu.sbatch job per node count (1, 2, 4, 8, 16,
# 32, 64, 128), chained with --dependency=afterok so they run one at a
# time and each gets its own results_zfp_compress_cpu_<jobid>.csv.
# Mirrors analysis_scripts/*_run.sh.

cd "$(dirname "$0")"

source ./common.sh
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
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE zfp_compress_cpu.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE --dependency=afterok:$prev_jid zfp_compress_cpu.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
