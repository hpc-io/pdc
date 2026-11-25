#!/bin/bash
# Run PDC servers and MPI clients as separate processes.

extra_cmd=""

if [[ "$SUPERCOMPUTER" == "perlmutter" ]]; then
    extra_cmd="--mem=25600 --cpu_bind=cores --overlap"
fi

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi
# check the test to be run:
test_exe="$1"
mpi_cmd="$2"
n_servers="$3"
n_client="$4"
# copy the remaining test input arguments (if any)
test_args="$5 $6 $7"
shift
echo "Input arguments are the followings"
echo $test_args
if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi
rm -rf pdc_tmp pdc_data
# START the server (in the background)
echo "$mpi_cmd -n $n_servers $extra_cmd ./pdc_server &"
$mpi_cmd -n $n_servers $extra_cmd ./pdc_server &
server_pid=$!
cleanup() {
    kill "$server_pid" 2>/dev/null || :
    wait "$server_pid" 2>/dev/null || :
}
trap cleanup EXIT
trap 'exit 143' TERM
# RUN the actual test
echo "$mpi_cmd -n $n_client $extra_cmd $test_exe $test_args"
$mpi_cmd -n $n_client $extra_cmd $test_exe $test_args
# Need to test the return value
ret="$?"
# and shutdown the SERVER before exiting
echo "Close server"
echo "$mpi_cmd -n $n_servers $extra_cmd ./close_server"
$mpi_cmd -n $n_servers $extra_cmd ./close_server
close_status=$?
if ((close_status != 0)); then
    if ((ret != 0)); then
        exit "$ret"
    fi
    exit "$close_status"
fi

wait "$server_pid"
server_status=$?
trap - EXIT TERM
if ((ret != 0)); then
    exit "$ret"
fi
exit "$server_status"
