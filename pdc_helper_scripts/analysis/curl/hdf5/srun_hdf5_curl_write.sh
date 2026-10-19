#!/bin/bash
# HDF5 curl benchmark, posthoc phase 1: run hdf5_bench_curl_write, which
# writes u/v/w into a fresh file and exits. See srun_hdf5_curl_analyze.sh
# (phase 2), run as a separate srun step once every rank here has
# exited -- see curl_hdf5_analysis.sbatch.
#
# Required env: BIN_DIR (hdf5_analysis_test/ directory, holding the built
#   hdf5_bench_curl_write binary), NUM_NODES, CLIENTS_PER_NODE,
#   CLIENT_TOTAL_TASKS, NX, NY, NZ_PER_RANK, LOG_TAG, WRITE_LOG, OUT_FILE

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_write_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_write_${NUM_NODES}.err" \
  ./hdf5_bench_curl_write "$NX" "$NY" "$NZ_PER_RANK" "$OUT_FILE"
popd

# hdf5_bench_curl_write.c now prints one CSV line per timestep
# (N_TIMESTEPS=3), so capture every matching line, not just the last.
lines=$(grep "^curl_posthoc_write," "$BIN_DIR/client_${LOG_TAG}_write_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "curl_posthoc_write,FAILED,${CLIENT_TOTAL_TASKS},${NX},${NY},${NZ_PER_RANK},FAILED" > "$WRITE_LOG"
else
  echo "$lines" > "$WRITE_LOG"
fi
