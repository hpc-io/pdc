#!/bin/bash
# Run vpic_bdcats (write 5 timesteps, then read every one back in the
# same process) against the server srun_server.sh already started in this
# same sbatch allocation. Mirrors
# pdc_helper_scripts/curl_analysis_scripts/srun_client_curl_eager.sh.
#
# Required env: BIN_DIR, NUM_NODES, CLIENTS_PER_NODE, CLIENT_TOTAL_TASKS,
#   NUMPARTICLES, STEPS, MODE, ASYNC_SLEEP_S, LOG_TAG, RESULTS_DIR, OUT_CSV
#
# HG_TRANSPORT/HG_HOST: see srun_server.sh -- Tuolumne's libfabric only has
# the cxi provider, not the "ofi+tcp" pdc_client_connect.c defaults to
# off-Perlmutter. HG_HOST is "cxi0:" (trailing colon, empty id field) --
# NOT "cxi0" with no colon (confirmed to make every rank fail HG_Init(),
# see srun_server.sh) and NOT an explicit non-empty id: an earlier version
# offset SLURM_LOCALID by SERVERS_PER_NODE (mirroring curl_analysis_scripts'
# Perlmutter HG_HOST=cxi0:$((SLURM_LOCALID + 8))), but SLURM_LOCALID is
# unbound under Tuolumne's Flux Slurm-compatibility shim, so every rank's
# arithmetic collapsed to the same literal id -- confirmed as the actual
# cause of a real run where every one of 8 client ranks failed HG_Init()
# with an identical "ofi+cxi://cxi0:4" connection string.

set -xeu

CLIENT_LOG="${RESULTS_DIR}/client_${LOG_TAG}_${NUM_NODES}.log"

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="$CLIENT_LOG" \
  --error="${RESULTS_DIR}/client_${LOG_TAG}_${NUM_NODES}.err" \
  bash -c 'export HG_TRANSPORT=ofi+cxi; export HG_HOST=cxi0:; exec ./vpic_bdcats "$NUMPARTICLES" "$STEPS" "$MODE" "$ASYNC_SLEEP_S"'
popd

# vpic_bdcats.c prints its CSV (header + api_call/api_call_write/
# api_call_read/throughput/data-size rows) interleaved with ordinary
# LOG_WARNING lines on rank 0's stdout -- keep only the CSV lines.
grep -E "^(record_type,|api_call,|api_call_write,|api_call_read,|throughput_write_MBps,|throughput_read_MBps,|total_data_size_bytes,|data_size_per_rank_bytes,)" \
  "$CLIENT_LOG" > "$OUT_CSV"
