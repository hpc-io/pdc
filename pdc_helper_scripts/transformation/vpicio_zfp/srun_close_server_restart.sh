#!/bin/bash

set -xeu

pushd $BIN_DIR
srun \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node=$DATA_SERVERS_PER_NODE \
  --error="close_server_restart_${TRANSFORM}_${NUM_NODES}.err" \
  --output="close_server_restart_${TRANSFORM}_${NUM_NODES}.log" \
  ./close_server
popd
