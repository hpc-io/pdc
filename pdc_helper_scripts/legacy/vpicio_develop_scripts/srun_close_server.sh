#!/bin/bash

set -xeu

pushd ../build/bin
srun -N $NUM_NODES -n $TOTAL_SERVER_TASKS --ntasks-per-node=$DATA_SERVERS_PER_NODE --error=close_server_output.err --output=close_server_output.log ./close_server
popd
