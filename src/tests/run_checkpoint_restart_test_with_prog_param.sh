#!/bin/bash
# This version of the test runner doesn't attempt to run any parallel tests.
# We assume too, that if the library build has enabled MPI, that LD_LIBRARY_PATH is
# defined and points to the MPI libraries used by the linker (e.g. -L<path> -lmpi)

# Each argument should be a quoted string containing the executable and its parameters.

# Usage:
#   ./run_checkpoint_restart_test_with_prog_param.sh \
#       "<test_exe1> [args...]" "<test_exe2> [args...]" ...
#
# Example:
#   ./run_checkpoint_restart_test_with_prog_param.sh \
#       "/path/to/kvtag_add_scale 1000 100" \
#       "/path/to/kvtag_get_verify_scale 1000 100"

# Cori CI needs srun even for serial tests
run_cmd=""

if [[ "$SUPERCOMPUTER" == "perlmutter" ]]; then
    run_cmd="srun -n 1 --mem=25600 --cpu_bind=cores --overlap"
fi

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1; fi

rm -rf pdc_data pdc_tmp

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
    echo "$run_cmd ./pdc_server $restart &"
    $run_cmd "./pdc_server" $restart &

    # WAIT a bit...
    sleep 1

    echo "testing: $test_exe $test_params"
    $run_cmd $test_exe $test_params
    ret="$?"

    # Shutdown the server before next iteration
    echo "$run_cmd ./close_server"
    $run_cmd "./close_server"

    restart="restart"
done

exit $ret