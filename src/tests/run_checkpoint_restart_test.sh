#!/bin/bash
# This version of the test runner doesn't attempt to run any parallel tests.
# We assume too, that if the library build has enabled MPI, that LD_LIBRARY_PATH is
# defined and points to the MPI libraries used by the linker (e.g. -L<path -lmpi)

# Cori CI needs srun even for serial tests
run_cmd=""
if [[ "$NERSC_HOST" == "perlmutter"  ]]; then
    run_cmd="srun -n 1 --mem=25600 --cpu_bind=cores --overlap"
fi

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi
# check the test to be run:
# test_exe="$1"
# shift
# copy the remaining test input arguments (if any)
test_args="$*"
rm -rf pdc_data pdc_tmp
# if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi
# RUN the actual test(s)
restart=" "
for test_exe in $test_args
do
    # START the server (in the background)
    echo "$run_cmd ./pdc_server.exe $restart &"
    $run_cmd ./pdc_server.exe $restart &
    # WAIT a bit...
    sleep 1
    echo "testing: $test_exe"
    $run_cmd $test_exe
    ret="$?"
    # and shutdown the SERVER before exiting
    echo "$run_cmd ./close_server"
    $run_cmd ./close_server
    restart="restart"
done
exit $ret
