#!/bin/bash
# Run the vpicio writer client. Required env: BIN_DIR, NUM_NODES,
#   CLIENT_TOTAL_TASKS, CLIENTS_PER_NODE, LOG_TAG, NPARTICLES, STEPS,
#   SLEEPTIME, TRANSFORM (one of vpicio.c's transformation_str values --
#   "zfp", "zfp_gpu", "zfp_libsod", "raw", etc.)

set -xeu

pushd "$BIN_DIR"
srun \
  --overlap \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --error="client_${LOG_TAG}_${NUM_NODES}.err" \
  --output="client_${LOG_TAG}_${NUM_NODES}.log" \
  ./vpicio "$NPARTICLES" "$STEPS" "$SLEEPTIME" "$TRANSFORM"
popd
