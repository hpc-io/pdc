#!/bin/bash

set -x
set -u

NUM_NODES=2

pushd ./build/bin
rm *.log *.err || true
srun -N $NUM_NODES -n $NUM_NODES --ntasks-per-node=1 --error="close_server_output.err" --output="close_server_output.log" ./close_server
popd
