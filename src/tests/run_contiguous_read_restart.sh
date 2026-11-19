#!/usr/bin/env bash
# Verify contiguous and partial reads from persisted data in a fresh server.
set -euo pipefail
pdc_test_bin=$(cd -- "$(dirname -- "$0")" && pwd)
pdc_test_dir=$(mktemp -d "$PWD/contiguous-read-XXXXXX")
export PDC_TMPDIR="$pdc_test_dir/pdc_tmp"
export PDC_DATA_LOC="$pdc_test_dir"
cd "$pdc_test_dir"
pdc_test_pid=
pdc_test_probe=${1:-}
cleanup() {
    if [[ -n "$pdc_test_pid" ]] && kill -0 "$pdc_test_pid" 2>/dev/null; then
        kill "$pdc_test_pid" 2>/dev/null || true
        wait "$pdc_test_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT
start_server() {
    local phase=$1
    shift
    : > "server-$phase.log"
    if [[ "$phase" == read && -n "$pdc_test_probe" ]]; then
        env LD_PRELOAD="$pdc_test_probe${LD_PRELOAD:+:$LD_PRELOAD}" \
            "$pdc_test_bin/pdc_server" "$@" > "server-$phase.log" 2>&1 &
    else
        "$pdc_test_bin/pdc_server" "$@" > "server-$phase.log" 2>&1 &
    fi
    pdc_test_pid=$!
    local deadline=$((SECONDS + 30))
    while ! grep -Eq 'Server ready(!| \(MPI disabled\))' "server-$phase.log"; do
        kill -0 "$pdc_test_pid" 2>/dev/null || { cat "server-$phase.log"; return 1; }
        (( SECONDS < deadline )) || { cat "server-$phase.log"; return 1; }
        sleep 0.05
    done
}
stop_server() {
    "$pdc_test_bin/close_server"
    wait "$pdc_test_pid"
    pdc_test_pid=
}
start_server write
"$pdc_test_bin/region_transfer_contiguous_read" write
stop_server
start_server read restart
"$pdc_test_bin/region_transfer_contiguous_read" read
stop_server
if grep -Fq '[ERROR]' server-write.log server-read.log; then
    cat server-write.log server-read.log
    exit 1
fi
