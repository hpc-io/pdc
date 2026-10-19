#!/bin/bash
# Run bench_magnitude in "lazy" mode: input objects are written as plain
# PDC objects with no graph attached to the write path, and magnitude is
# only computed on the first read of it (transparently, server-side),
# within the same continuous client session. See
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
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./bench_magnitude lazy "$N_ELEM"'
popd

# bench_magnitude.c now prints one CSV line per timestep (N_TIMESTEPS=3),
# so capture every matching line, not just the last.
lines=$(grep "^lazy," "$BIN_DIR/client_${LOG_TAG}_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "lazy,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" >> "$RESULTS"
else
  echo "$lines" >> "$RESULTS"
fi
