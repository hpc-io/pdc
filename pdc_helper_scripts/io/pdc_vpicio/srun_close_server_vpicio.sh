#!/bin/bash
# Cleanly close the PDC server(s) for this job -- close_server.c's
# PDC_Client_close_all_server RPC checkpoints every server's in-memory
# metadata to disk before exiting, and prints one "total close time = X"
# line per server rank to this step's own log. That log is what
# vpicio_raw.sbatch greps afterward to populate avg_close_s /
# total_with_close_s, the same convention transformation/vpicio_zfp/ uses.
#
# Required env: BIN_DIR, NUM_NODES, SERVER_TOTAL_TASKS,
#   SERVERS_PER_NODE, LOG_TAG

set -xeu

pushd "$BIN_DIR"
srun \
  --overlap \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node="$SERVERS_PER_NODE" \
  --error="close_server_${LOG_TAG}_${NUM_NODES}.err" \
  --output="close_server_${LOG_TAG}_${NUM_NODES}.log" \
  ./close_server
popd
