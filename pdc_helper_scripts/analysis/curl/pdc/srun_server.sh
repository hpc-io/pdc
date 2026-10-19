#!/bin/bash
# Start pdc_server in the background for this job's node count. Mirrors
# analysis_scripts/srun_server.sh: 8 data servers/node, decoupled from the
# client rank count. static_region_partition splits each object across
# however many data servers exist regardless of client count, so this
# doesn't need to match the client rank count for correctness. Shutdown
# is graceful via srun_close_server.sh (the close_server client RPC), not
# a captured PID -- the srun step outlives this script under the same
# Slurm job allocation.
#
# Required env: BIN_DIR, PDC_DATA_LOC, NUM_NODES, SERVERS_PER_NODE,
#   SERVER_TOTAL_TASKS, LOG_TAG

set -xeu

pushd "$BIN_DIR"
# pdc_server (launched without the `restart` argument, as below) never
# consults the checkpoint file or any other on-disk state -- it always
# starts with an empty in-memory metadata table (see PDC_Server_init in
# src/server/pdc_server.c: checkpoint loading is gated purely on argv[1]
# == "restart", with no existence check). But its actual per-rank state
# (checkpoint files under $PDC_TMPDIR/<rank>/, and the top-level
# discovery/address config file directly under $PDC_TMPDIR) still lives on
# disk from any previous run against this same PDC_DATA_LOC, and
# PDC_TMPDIR is set equal to PDC_DATA_LOC by the caller -- neither of
# those paths is named "pdc_data" or "pdc_tmp", so an rm -rf targeting
# only those two subdirectory names removes nothing real (see
# analysis_scripts/srun_server.sh, which had the same bug). Clear the
# whole directory's contents so a fresh, non-restart launch can never
# observe another run's address file or (if `restart` is ever used here
# by mistake) checkpoint.
rm -rf "${PDC_DATA_LOC:?}"/*
srun \
  --overlap \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node="$SERVERS_PER_NODE" \
  --error="server_${LOG_TAG}_${NUM_NODES}.err" \
  --output="server_${LOG_TAG}_${NUM_NODES}.log" \
  bash -c 'export HG_HOST=cxi0:$SLURM_LOCALID; exec ./pdc_server' &
popd

# Give the servers time to stand up and publish their address info before
# any client tries to connect.
sleep 10
