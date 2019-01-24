#!/bin/bash

if [ $# -lt 1 ]; then echo "missing test argument" && exit -1 ; fi
# check the test to be run:
test_exe="$1"
shift
# copy the remaining test input arguments (if any)
test_args="$*"
if [ -x $test_exe ]; then echo "testing: $test_exe"; else echo "test: $test_exe not found or not and executable" && exit -2; fi
./pdc_server.exe &
sleep 1
$test_exe $*
./close_server
