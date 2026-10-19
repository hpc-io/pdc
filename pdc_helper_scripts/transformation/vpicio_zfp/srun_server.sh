#!/bin/bash

set -xeu

pushd $BIN_DIR
rm -rf $PDC_DATA_LOC/pdc_data
srun \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node=$DATA_SERVERS_PER_NODE \
  --error="server_output_${1}_${TRANSFORM}_${NUM_NODES}.err" \
  --output="server_output_${1}_${TRANSFORM}_${NUM_NODES}.log" \
  ./pdc_server &
popd
