#!/usr/bin/env bash
# Preserve read contents and caller ownership if segmented registration is unavailable.
set -euo pipefail
pdc_test_bin=$(cd -- "$(dirname -- "$0")" && pwd)
pdc_test_fixture=$1
pdc_test_dir=$(mktemp -d "$PWD/segmented-fallback-XXXXXX")
export PDC_TMPDIR="$pdc_test_dir/pdc_tmp" PDC_DATA_LOC="$pdc_test_dir"
cd "$pdc_test_dir"
: > server.log
"$pdc_test_bin/pdc_server" > server.log 2>&1 &
pdc_test_pid=$!
cleanup() {
    if kill -0 "$pdc_test_pid" 2>/dev/null; then
        kill "$pdc_test_pid" 2>/dev/null || true
        wait "$pdc_test_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT
pdc_test_deadline=$((SECONDS + 30))
while [[ $(<server.log) != *"Server ready!"* ]]; do
    kill -0 "$pdc_test_pid" 2>/dev/null || { cat server.log; exit 1; }
    (( SECONDS < pdc_test_deadline )) || { cat server.log; exit 1; }
    sleep 0.05
done
if ! LD_PRELOAD="$pdc_test_fixture${LD_PRELOAD:+:$LD_PRELOAD}" "$pdc_test_bin/region_transfer_contiguous_read" > client.log 2>&1; then
    cat client.log server.log
    exit 1
fi
"$pdc_test_bin/close_server"
wait "$pdc_test_pid"
grep -Fq 'PDC test: rejected segmented registration' client.log
if grep -Fq '[ERROR]' client.log server.log; then
    cat client.log server.log
    exit 1
fi
cat client.log
