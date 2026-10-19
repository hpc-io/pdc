#!/bin/bash
# Gracefully shut down the pdc_server step started by srun_server.sh, via
# the close_server client RPC (PDC_Client_close_all_server) -- same pattern
# as vpicio_scripts/srun_close_server.sh.
#
# Required env: BIN_DIR, NUM_NODES, SERVERS_PER_NODE, SERVER_TOTAL_TASKS,
#   LOG_TAG

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node="$SERVERS_PER_NODE" \
  --error="close_server_${LOG_TAG}_${NUM_NODES}.err" \
  --output="close_server_${LOG_TAG}_${NUM_NODES}.log" \
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./close_server'
popd

# Let the backgrounded server step fully exit before the job script moves
# on.
sleep 2
