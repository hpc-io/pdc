#!/bin/bash
# Scale study: cache ON, 32-256 clients, 8 nodes
# Estimated: ~3 tests/config * 3 strategies * ~120s each = ~18 min
NPARTICLES=8388608
STEPS=5
SLEEP_TIME=20
BUILD_DIR=/mnt/fast/nlewis/workspace/source/pdc/build
MPIEXEC=${MPIEXEC:-mpiexec}
NPROC=$(nproc)

cd $BUILD_DIR
cmake .. -DPDC_SERVER_CACHE=ON > /dev/null && make -j${NPROC} 2>&1 | tail -3

run_test() {
    local n_servers=$1 n_clients=$2 strategy=$3
    cd $BUILD_DIR/bin
    rm -rf pdc_tmp pdc_data
    $MPIEXEC -n $n_servers ./pdc_server &
    sleep 2
    echo "=== cache=1 servers=${n_servers} clients=${n_clients} strategy=${strategy} ==="
    $MPIEXEC -n $n_clients ./vpicio $NPARTICLES $STEPS $SLEEP_TIME $strategy
    [ $? -ne 0 ] && echo "FAILED"
    $MPIEXEC -n $n_servers ./close_server
    sleep 1
}

echo "### cache=ON: fixed 8 servers, scale clients 32-256 ###"
for n_clients in 32 64 128 256; do
    for strategy in 0 1 2; do
        run_test 8 $n_clients $strategy
    done
done
