#!/bin/bash
# Run the parallel-HDF5 posthoc analyze phase
# (hdf5_analysis_test/hdf5_bench_posthoc_analyze): reopen the file a
# prior, already-exited write-phase job created, read vx/vy/vz back,
# compute magnitude client-side, and write it back as a fourth dataset.
# Run as its own srun step (a genuinely new set of processes, not a
# continuation of the write phase) -- see hdf5_analysis.sbatch.
#
# Required env: BIN_DIR (hdf5_analysis_test/ directory, holding the built
#   hdf5_bench_posthoc_analyze binary), NUM_NODES, CLIENTS_PER_NODE,
#   CLIENT_TOTAL_TASKS, N_ELEM, LOG_TAG, ANALYZE_LOG, OUT_FILE (path
#   written by srun_hdf5_write.sh)

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_analyze_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_analyze_${NUM_NODES}.err" \
  ./hdf5_bench_posthoc_analyze "$N_ELEM" "$OUT_FILE"
popd

# hdf5_bench_posthoc_analyze.c now prints one CSV line per timestep
# (N_TIMESTEPS=3), so capture every matching line, not just the last.
lines=$(grep "^posthoc_analyze," "$BIN_DIR/client_${LOG_TAG}_analyze_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "posthoc_analyze,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" > "$ANALYZE_LOG"
else
  echo "$lines" > "$ANALYZE_LOG"
fi
