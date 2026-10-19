#!/bin/bash
# HDF5 curl benchmark, posthoc phase 2: run hdf5_bench_curl_analyze, a
# freshly launched client that reopens the file a prior, already-exited
# write-phase job created, reads u/v/w back, computes curl then
# vorticity magnitude client-side, and writes curl_x/y/z and
# vorticity_magnitude back as four datasets.
#
# Required env: BIN_DIR (hdf5_analysis_test/ directory, holding the built
#   hdf5_bench_curl_analyze binary), NUM_NODES, CLIENTS_PER_NODE,
#   CLIENT_TOTAL_TASKS, NX, NY, NZ_PER_RANK, LOG_TAG, ANALYZE_LOG, OUT_FILE

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_analyze_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_analyze_${NUM_NODES}.err" \
  ./hdf5_bench_curl_analyze "$NX" "$NY" "$NZ_PER_RANK" "$OUT_FILE"
popd

# hdf5_bench_curl_analyze.c now prints one CSV line per timestep
# (N_TIMESTEPS=3), so capture every matching line, not just the last.
lines=$(grep "^curl_posthoc_analyze," "$BIN_DIR/client_${LOG_TAG}_analyze_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "curl_posthoc_analyze,FAILED,${CLIENT_TOTAL_TASKS},${NX},${NY},${NZ_PER_RANK},FAILED" > "$ANALYZE_LOG"
else
  echo "$lines" > "$ANALYZE_LOG"
fi
