#!/bin/bash
# Run bench_magnitude in "eager" mode: input objects are written through
# the region-analysis framework (DataFlyway) with the vector_magnitude
# graph attached, so the last input write transparently triggers
# server-side magnitude computation in the write path. See
# src/tests/analysis/bench_magnitude.c for the full timing breakdown.
#
# Required env: BIN_DIR, NUM_NODES, CLIENTS_PER_NODE, CLIENT_TOTAL_TASKS,
#   N_ELEM, LOG_TAG, RESULTS

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_${NUM_NODES}.err" \
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./bench_magnitude eager "$N_ELEM"'
popd

# bench_magnitude.c now prints one CSV line per timestep (N_TIMESTEPS=3),
# so capture every matching line, not just the last.
lines=$(grep "^eager," "$BIN_DIR/client_${LOG_TAG}_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "eager,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" >> "$RESULTS"
else
  echo "$lines" >> "$RESULTS"
fi
