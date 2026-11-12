#!/bin/bash
# MPI version of the checkpoint/restart test runner with program parameters.
# We assume that if the library build has enabled MPI, that LD_LIBRARY_PATH is
# defined and points to the MPI libraries used by the linker (e.g. -L<path> -lmpi)

# Usage:
#   ./run_checkpoint_restart_mpi_test_with_prog_param.sh <mpi_cmd> <n_servers> <n_client> \
#       "<test_exe1> [args...]" "<test_exe2> [args...]" ...
#
# Example:
#   ./run_checkpoint_restart_mpi_test_with_prog_param.sh srun 2 4 \
#       "/path/to/kvtag_add_scale 1000 100" \
#       "/path/to/kvtag_get_verify_scale 1000 100"

extra_cmd=""

if [[ "$SUPERCOMPUTER" == "perlmutter" ]]; then
    extra_cmd="--mem=25600 --cpu_bind=cores --overlap"
fi

if [ $# -lt 4 ]; then
    echo "Usage: $0 <mpi_cmd> <n_servers> <n_client> \"<test_exe> [args...]\" ..."
    exit -1
fi

mpi_cmd="$1"
n_servers="$2"
n_client="$3"
shift 3

rm -rf pdc_tmp pdc_data

restart=" "
ret=0
for test_args in "$@"
do
    # Split the quoted string into the executable and its parameters
    read -ra cmd_parts <<< "$test_args"
    test_exe="${cmd_parts[0]}"
    test_params="${cmd_parts[@]:1}"

    if [ ! -x "$test_exe" ]; then
        echo "test: $test_exe not found or not an executable" && exit -2
    fi

    # START the server (in the background)
    echo "$mpi_cmd -n $n_servers -c 2 $extra_cmd ./pdc_server $restart &"
    $mpi_cmd -n $n_servers -c 2 $extra_cmd "./pdc_server" $restart &

    # WAIT a bit...
    sleep 1

    # RUN the actual test
    echo "testing: $test_exe $test_params"
    echo "$mpi_cmd -n $n_client -c 2 $extra_cmd $test_exe $test_params"
    $mpi_cmd -n $n_client -c 2 $extra_cmd $test_exe $test_params
    ret="$?"

    # Shutdown the server before next iteration
    echo "Close server"
    echo "$mpi_cmd -n $n_servers -c 2 $extra_cmd ./close_server"
    $mpi_cmd -n $n_servers -c 2 $extra_cmd "./close_server"

    restart="restart"
done

exit $ret