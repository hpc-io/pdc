#!/bin/bash

set -x
set -u

NUM_NODES=2

pushd ./build/bin
rm *.log *.err || true
srun -N $NUM_NODES -n $NUM_NODES --ntasks-per-node=1 --error="server_output.err" --output="server_output.log" ./pdc_server &
popd
