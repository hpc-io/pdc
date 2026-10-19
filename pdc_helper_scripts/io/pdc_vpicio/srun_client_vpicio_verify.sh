#!/bin/bash
# Run vpicio_verify as its own, genuinely separate srun step, launched
# only after srun_client_vpicio.sh's writer step has fully exited -- so
# read/verification time is never folded into the write-workload CSV
# (see vpicio.c's and vpicio_verify.c's own header comments). Its output
# goes to its own log, never parsed into results_vpicio_raw_<jobid>.csv.
#
# Required env: BIN_DIR, NUM_NODES, CLIENT_TOTAL_TASKS, CLIENTS_PER_NODE,
#   LOG_TAG, NPARTICLES, STEPS

set -xeu

pushd "$BIN_DIR"
srun \
  --overlap \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --error="verify_${LOG_TAG}_${NUM_NODES}.err" \
  --output="verify_${LOG_TAG}_${NUM_NODES}.log" \
  ./vpicio_verify "$NPARTICLES" "$STEPS"
popd
