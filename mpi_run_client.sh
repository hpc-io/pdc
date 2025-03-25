#!/bin/bash

./rebuild.sh  # Rebuild project

pushd ./build/bin || exit 1  # Exit if cd fails
mpirun -np $1 "./transform_write_data_in_1D"
popd

