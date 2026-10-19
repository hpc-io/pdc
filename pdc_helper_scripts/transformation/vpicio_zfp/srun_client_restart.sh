#!/bin/bash

set -xeu

pushd $BIN_DIR
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output=client_output_restart_${TRANSFORM}_${NUM_NODES}.log \
  --error=client_output_restart_${TRANSFORM}_${NUM_NODES}.err \
  ./bdcats_mts 8388608 5 20
popd

