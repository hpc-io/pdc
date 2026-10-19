#!/bin/bash
# Submits one curl_eager_transient_analysis.sbatch job per node count (1, 2,
# 4, 8, 16, 32, 64, 128 -- 32 ranks/node), chained with --dependency=afterok
# so they run one at a time, each gets its own results_curl_eager_transient_<jobid>.csv.
# Pinned to 4 PDC servers per node (the transient-curl variant is only run
# at this server count) -- see SERVER_SWEEP below.

cd "$(dirname "$0")"

source ../../common.sh
# Transient-curl variant runs at 4 servers/node only.
export SERVER_SWEEP=4
# Allocate task slots/node for the one server count plus the clients
# (the sbatch script itself also pins SERVER_SWEEP=4 internally).
SWEEP_MAX=$(printf "%s\n" $SERVER_SWEEP | sort -n | tail -1)
NTASKS_PER_NODE=$((SWEEP_MAX + CLIENTS_PER_NODE))

prev_jid=""
for nodes in 1 2 4 8 16 32 64 128; do
    if [ -z "$prev_jid" ]; then
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE curl_eager_transient_analysis.sbatch | awk '{print $4}')
    else
        jid=$(sbatch --nodes=$nodes --ntasks-per-node=$NTASKS_PER_NODE --dependency=afterok:$prev_jid curl_eager_transient_analysis.sbatch | awk '{print $4}')
    fi
    echo "Submitted job $jid with $nodes nodes"
    prev_jid=$jid
done
