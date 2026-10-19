#!/bin/bash

set -xeu

pushd ../build/bin
rm *.log *.err || true
srun -N $NUM_NODES -n $SERVER_TOTAL_TASKS --ntasks-per-node=$DATA_SERVERS_PER_NODE --error=server_output.err --output=server_output.log ./pdc_server &
popd
