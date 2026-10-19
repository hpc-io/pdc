#!/bin/bash
# Run bench_curl_eager: writes u,v,w through the region-analysis
# framework (DataFlyway) with the curl_vorticity_magnitude graph
# attached, so the last input write transparently triggers server-side
# curl + vector_magnitude computation (and, with COMPRESS=1, GPU ZFP
# compression of vorticity_magnitude) in the write path. See
# src/tests/analysis/bench_curl_eager.c for the full timing breakdown.
#
# Required env: BIN_DIR, NUM_NODES, CLIENTS_PER_NODE, CLIENT_TOTAL_TASKS,
#   NX, NY, NZ_PER_RANK, COMPRESS, LOG_TAG, RESULTS (TRANSIENT optional,
#   defaults to 0 -- 1 selects the transient-curl graph, see bench_curl_eager.c)

set -xeu

pushd "$BIN_DIR"
srun \
  --overlap \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_${LOG_TAG}_${NUM_NODES}.log" \
  --error="client_${LOG_TAG}_${NUM_NODES}.err" \
  bash -c 'export HG_HOST=cxi0:$((SLURM_LOCALID + 8)); exec ./bench_curl_eager "$NX" "$NY" "$NZ_PER_RANK" "$COMPRESS" "${TRANSIENT:-0}"'
popd

# bench_curl_eager.c now prints one CSV line per timestep (N_TIMESTEPS=3),
# so capture every matching line, not just the last.
lines=$(grep "^curl_eager," "$BIN_DIR/client_${LOG_TAG}_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "curl_eager,FAILED,${CLIENT_TOTAL_TASKS},${NX},${NY},${NZ_PER_RANK},${COMPRESS},FAILED" >> "$RESULTS"
else
  echo "$lines" >> "$RESULTS"
fi
