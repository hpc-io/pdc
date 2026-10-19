#!/bin/bash
# Run the parallel-HDF5 write phase (hdf5_analysis_test/hdf5_bench_write):
# write vx/vy/vz into a fresh, pre-striped file and exit. See
# srun_hdf5_posthoc_analyze.sh for phase 2, run as a separate srun step
# once every rank here has exited -- see hdf5_analysis.sbatch.
#
# Required env: BIN_DIR (hdf5_analysis_test/ directory, holding the built
#   hdf5_bench_write binary), NUM_NODES, CLIENTS_PER_NODE,
#   CLIENT_TOTAL_TASKS, N_ELEM, LOG_TAG, WRITE_LOG, OUT_FILE (pre-striped
#   path this benchmark writes to -- see hdf5_analysis.sbatch)

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_write_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_write_${NUM_NODES}.err" \
  ./hdf5_bench_write "$N_ELEM" "$OUT_FILE"
popd

# hdf5_bench_write.c now prints one CSV line per timestep (N_TIMESTEPS=3),
# so capture every matching line, not just the last.
lines=$(grep "^posthoc_write," "$BIN_DIR/client_${LOG_TAG}_write_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "posthoc_write,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" > "$WRITE_LOG"
else
  echo "$lines" > "$WRITE_LOG"
fi
