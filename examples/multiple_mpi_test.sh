#!/bin/bash
# This version of the test runner doesn't attempt to run any parallel tests.
# We assume too, that if the library build has enabled MPI, that LD_LIBRARY_PATH is
# defined and points to the MPI libraries used by the linker (e.g. -L<path -lmpi)

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi

extra_cmd=""

if [[ "$HOSTNAME" == "cori"* || "$HOSTNAME" == "nid"* ]]; then
    extra_cmd="--mem=25600 --cpu_bind=cores --overlap "
fi
# check the test to be run:
# test_exe="$1"
# shift
# copy the remaining test input arguments (if any)
test_exe1="$1"
test_exe2="$2"
mpi_cmd="$3"
n_servers="$4"
n_client="$5"
test_args="$6 $7 $8"
# if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi
# START the server (in the background)
echo "$mpi_cmd -n $n_servers $extra_cmd ./pdc_server.exe &"
$mpi_cmd -n $n_servers $extra_cmd $PDC_DIR/bin/./pdc_server.exe &
# WAIT a bit...
sleep 1
# RUN the actual test(s)
echo "$mpi_cmd -n $n_client $extra_cmd $test_exe1 $test_args"
$mpi_cmd -n $n_client $extra_cmd $test_exe1 $test_args
sleep 1
echo "$mpi_cmd -n $n_client $extra_cmd $test_exe2 $test_args"
$mpi_cmd -n $n_client $extra_cmd $test_exe2 $test_args
# and shutdown the SERVER before exiting
$mpi_cmd -n 1 $PDC_DIR/bin/./close_server
