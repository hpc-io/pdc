#!/bin/bash
# Restart pdc_server for this job's node count, passing the `restart`
# argument so it reloads the metadata checkpoint written when
# srun_close_server.sh closed the previous server instance -- see
# PDC_Server_restart / PDC_Server_checkpoint in src/server/pdc_server.c.
# Used between the write and analyze phases of the posthoc benchmark so
# the analyze phase talks to a server process that genuinely had to
# reload persisted data, not one that just kept it in memory the whole
# time. Unlike srun_server.sh, this does NOT wipe $PDC_DATA_LOC first --
# that would delete the checkpoint it's about to read.
#
# Required env: BIN_DIR, NUM_NODES, SERVERS_PER_NODE, SERVER_TOTAL_TASKS,
#   LOG_TAG

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$SERVER_TOTAL_TASKS" \
  --ntasks-per-node="$SERVERS_PER_NODE" \
  --error="server_restart_${LOG_TAG}_${NUM_NODES}.err" \
  --output="server_restart_${LOG_TAG}_${NUM_NODES}.log" \
  bash -c 'export HG_HOST=cxi0:$SLURM_LOCALID; exec ./pdc_server restart' &
popd

# Give the servers time to stand up and reload the checkpoint before any
# client tries to connect.
sleep 10
