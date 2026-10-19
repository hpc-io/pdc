#!/bin/bash
# Posthoc benchmark, phase 2: run bench_posthoc_analyze, a freshly
# launched client that opens the vx/vy/vz objects a prior, already-exited
# write-phase job created, reads them back, computes magnitude
# client-side, and writes it back as a plain PDC object. Must run after
# srun_server_restart.sh so the server it's talking to actually reloaded
# that data from its checkpoint rather than just keeping it in memory.
#
# Required env: BIN_DIR, NUM_NODES, CLIENTS_PER_NODE, CLIENT_TOTAL_TASKS,
#   N_ELEM, LOG_TAG, ANALYZE_LOG

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_analyze_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_analyze_${NUM_NODES}.err" \
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./bench_posthoc_analyze "$N_ELEM"'
popd

# bench_posthoc_analyze.c now prints one CSV line per timestep
# (N_TIMESTEPS=3), so capture every matching line, not just the last.
lines=$(grep "^posthoc_analyze," "$BIN_DIR/client_${LOG_TAG}_analyze_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "posthoc_analyze,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" > "$ANALYZE_LOG"
else
  echo "$lines" > "$ANALYZE_LOG"
fi
