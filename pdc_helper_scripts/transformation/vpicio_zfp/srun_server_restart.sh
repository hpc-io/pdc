#!/bin/bash

set -xeu

pushd $BIN_DIR
srun \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node=$DATA_SERVERS_PER_NODE \
  --error="server_output_${1}_${TRANSFORM}_${NUM_NODES}.err" \
  --output="server_output_${1}_${TRANSFORM}_${NUM_NODES}.log" \
  ./pdc_server restart &
popd
