#!/bin/bash
# Gracefully shut down the pdc_server step started by srun_server.sh, via
# the close_server client RPC. Mirrors
# pdc_helper_scripts/curl_analysis_scripts/srun_close_server.sh.
#
# Required env: BIN_DIR, NUM_NODES, SERVERS_PER_NODE, SERVER_TOTAL_TASKS,
#   LOG_TAG, RESULTS_DIR
#
# HG_TRANSPORT/HG_HOST: see srun_server.sh -- close_server is a client-role
# binary too (pdc_client_connect.c), same cxi-only fix, "cxi0:" (trailing
# colon, empty id field -- NOT "cxi0" with no colon at all, see
# srun_server.sh for why that form fails HG_Init()).

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node="$SERVERS_PER_NODE" \
  --error="${RESULTS_DIR}/close_server_${LOG_TAG}_${NUM_NODES}.err" \
  --output="${RESULTS_DIR}/close_server_${LOG_TAG}_${NUM_NODES}.log" \
  bash -c 'export HG_TRANSPORT=ofi+cxi; export HG_HOST=cxi0:; exec ./close_server'
popd

# Let the backgrounded server step fully exit before the job script
# moves on.
sleep 2
