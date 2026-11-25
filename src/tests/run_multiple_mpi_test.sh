#!/bin/bash
# Run PDC servers and MPI clients as separate processes.

extra_cmd=""

if [[ "$SUPERCOMPUTER" == "perlmutter" ]]; then
    extra_cmd="--mem=25600 --cpu_bind=cores --overlap"
fi

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi
# check the test to be run:
mpi_cmd="$1"
n_servers="$2"
n_client="$3"
# copy the remaining test input arguments (if any)
all_test="${@:4}"
test_args=""
echo $all_test

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

# WAIT a bit, for 1 second
sleep 1

# RUN the actual test(s)
ret=0
for test_exe in $all_test
do
    if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi
    if [[ "$test_exe" = *vpicio ]]; then
        test_args="1024"
    fi
    if [[ "$test_exe" = *bdcats ]]; then
        test_args="1024"
    fi
    echo "$mpi_cmd -n $n_client $extra_cmd $test_exe $test_args"
    $mpi_cmd -n $n_client $extra_cmd $test_exe $test_args
    ret="$?"
    if ((ret != 0)); then
        break
    fi
done
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
