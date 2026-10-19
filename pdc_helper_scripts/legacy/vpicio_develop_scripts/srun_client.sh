#!/bin/bash

set -xeu

pushd ../build/bin
srun -N $NUM_NODES -n $CLIENT_TOTAL_TASKS --ntasks-per-node=$CLIENTS_PER_NODE --output=client_output.log --error=client_output.err ./vpicio_mts 8388608 5 20
popd

