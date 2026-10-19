#!/bin/bash
# eager_posthoc benchmark, phase 2: run bench_posthoc_analyze_eager, a
# freshly launched client that attaches the DataFlyway vector_magnitude
# graph to the vx/vy/vz objects a prior, already-exited write-phase job
# created (plain PDCobj_open, no graph involved at write time) plus a new
# magnitude object, then reads magnitude back -- that read is what
# triggers server-side eager computation via
# PDC_Server_data_io_region_analysis's read-side hook, since the
# write-side trigger bench_magnitude.c's eager mode relies on never fires
# here (the graph didn't exist in the process that did the writes). Must
# run after srun_server_restart.sh so the server it's talking to actually
# reloaded that data from its checkpoint rather than just keeping it in
# memory. See bench_posthoc_analyze_eager.c's header comment for the full
# mechanism.
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
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./bench_posthoc_analyze_eager "$N_ELEM"'
popd

# bench_posthoc_analyze_eager.c prints one CSV line per timestep
# (N_TIMESTEPS=3), so capture every matching line, not just the last.
lines=$(grep "^posthoc_analyze," "$BIN_DIR/client_${LOG_TAG}_analyze_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "posthoc_analyze,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" > "$ANALYZE_LOG"
else
  echo "$lines" > "$ANALYZE_LOG"
fi
