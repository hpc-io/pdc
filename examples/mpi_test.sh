#!/bin/bash
# This version of the test runner doesn't attempt to run any parallel tests.
# We assume too, that if the library build has enabled MPI, that LD_LIBRARY_PATH is
# defined and points to the MPI libraries used by the linker (e.g. -L<path -lmpi)

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi
# check the test to be run:
test_exe="$1"
mpi_cmd="$2"
n_servers="$3"
n_client="$4"
# copy the remaining test input arguments (if any)
test_args="$5 $6 $7 $8"
shift
echo "Input arguments are the followings"
echo $test_args
if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi
rm -rf pdc_tmp
# START the server (in the background)
echo "$mpi_cmd -n $n_servers ./pdc_server.exe &"
$mpi_cmd -n $n_servers $PDC_DIR/bin/./pdc_server.exe &
# WAIT a bit, for 1 second
sleep 1
# RUN the actual test
echo "$mpi_cmd -n $n_client $test_exe $test_args"
$mpi_cmd -n $n_client $test_exe $test_args
# Need to test the return value
ret="$?"
# and shutdown the SERVER before exiting
echo $mpi_cmd -n 1 ./close_server
$mpi_cmd -n 1 $PDC_DIR/bin/./close_server
exit $ret
