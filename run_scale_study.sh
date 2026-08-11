#!/bin/bash
# Usage: ./run_scale_study.sh [sleep_seconds]
# Runs scale study with and without cache, with and without sleep

SLEEP=${1:-0}
NPARTICLES=8388608
STEPS=5
SLEEP_TIME=20
BUILD_DIR=/mnt/fast/nlewis/workspace/source/pdc/build
SRC_DIR=/mnt/fast/nlewis/workspace/source/pdc
MPIEXEC=${MPIEXEC:-mpiexec}
NPROC=$(nproc)

rebuild() {
    local cache=$1
    echo "=== Rebuilding with PDC_SERVER_CACHE=${cache} ==="
    cd $BUILD_DIR
    cmake .. -DPDC_SERVER_CACHE=${cache} > /dev/null
    make -j${NPROC} 2>&1 | tail -3
}

run_test() {
    local n_servers=$1
    local n_clients=$2
    local strategy=$3
    local cache_label=$4

    cd $BUILD_DIR/bin
    rm -rf pdc_tmp pdc_data
    $MPIEXEC -n $n_servers ./pdc_server &
    sleep 2

    echo "=== cache=${cache_label} servers=${n_servers} clients=${n_clients} strategy=${strategy} sleep=${SLEEP} ==="
    $MPIEXEC -n $n_clients ./vpicio $NPARTICLES $STEPS $SLEEP_TIME $strategy
    [ $? -ne 0 ] && echo "FAILED"

    $MPIEXEC -n $n_servers ./close_server
    sleep 1
}

for cache in ON OFF; do
    rebuild $cache
    label=$([ "$cache" = "ON" ] && echo "1" || echo "0")

    echo "### Scale: 16 clients, 1-4 servers, cache=${label} ###"
    for n_servers in 1 2 4; do
        for strategy in 0 1 2; do
            run_test $n_servers 16 $strategy $label
        done
    done

    echo "### Scale: 1 server, 1-32 clients, cache=${label} ###"
    for n_clients in 1 2 4 8 16 32; do
        for strategy in 0 1 2; do
            run_test 1 $n_clients $strategy $label
        done
    done
done
