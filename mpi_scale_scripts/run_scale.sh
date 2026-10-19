#!/bin/bash
# Submits one mpi_object_setup_scale.sbatch job per node count (1, 2, 4,
# 8, 16, 32, 64, 128 -- 32 ranks/node, so 32 up to 4096 ranks total),
# chained with --dependency=afterok so they run one at a time. Each job
# writes its own results_mpi_object_setup_scale_<jobid>.csv; cat them
# together afterward to see the full sweep. Mirrors
# curl_analysis_scripts/*_run.sh.

cd "$(dirname "$0")"

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes mpi_object_setup_scale.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --dependency=afterok:$prev_jid mpi_object_setup_scale.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
