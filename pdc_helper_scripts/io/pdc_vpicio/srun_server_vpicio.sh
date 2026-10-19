#!/bin/bash
# Start pdc_server in the background for this job's node count.
# static_region_partition splits each object across however many data
# servers exist regardless of client count, so SERVERS_PER_NODE doesn't
# need to match CLIENTS_PER_NODE for correctness. Shutdown is graceful
# via srun_close_server_vpicio.sh (the close_server client RPC), not a
# captured PID -- the srun step outlives this script under the same
# Slurm job allocation.
#
# Required env: BIN_DIR, PDC_DATA_LOC, NUM_NODES, SERVERS_PER_NODE,
#   SERVER_TOTAL_TASKS, LOG_TAG
#
# --overlap: same fix applied in transformation/vpicio_zfp/ -- without
# it, concurrent srun steps within one job allocation don't reliably
# share it, and the server (backgrounded here) and the client srun
# started right after it can hang waiting for node resources that are,
# in reality, already available.

set -xeu

pushd "$BIN_DIR"
rm -rf "${PDC_DATA_LOC:?}"/*
srun \
  --overlap \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node="$SERVERS_PER_NODE" \
  --error="server_${LOG_TAG}_${NUM_NODES}.err" \
  --output="server_${LOG_TAG}_${NUM_NODES}.log" \
  ./pdc_server &
popd

# Give the servers time to stand up and publish their address info before
# any client tries to connect.
sleep 10
