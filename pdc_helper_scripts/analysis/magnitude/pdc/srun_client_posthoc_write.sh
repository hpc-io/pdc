#!/bin/bash
# Posthoc benchmark, phase 1: run bench_write_components, which writes
# vx/vy/vz as plain PDC objects (no graph attached) and exits. Every
# client rank from this srun step fully terminates before phase 2
# (srun_client_posthoc_analyze.sh) launches as a separate step, against a
# server that's been closed and restarted in between (see
# posthoc_analysis.sbatch) -- that relaunch is the part that makes this a
# real posthoc workflow instead of lazy's single continuous session.
#
# Required env: BIN_DIR, NUM_NODES, CLIENTS_PER_NODE, CLIENT_TOTAL_TASKS,
#   N_ELEM, LOG_TAG, WRITE_LOG

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_write_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_write_${NUM_NODES}.err" \
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./bench_write_components "$N_ELEM"'
popd

# bench_write_components.c now prints one CSV line per timestep
# (N_TIMESTEPS=3), so capture every matching line, not just the last.
lines=$(grep "^posthoc_write," "$BIN_DIR/client_${LOG_TAG}_write_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "posthoc_write,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" > "$WRITE_LOG"
else
  echo "$lines" > "$WRITE_LOG"
fi
