#!/bin/bash
# Run the single-session HighFive magnitude benchmark
# (hdf5_analysis_test/magnitude_highfive): write vx/vy/vz, read them
# back, compute magnitude, write it back, confirm-read it, all in one
# process -- unlike hdf5_bench_write/hdf5_bench_posthoc_analyze, there's
# no separate write/analyze phase to chain (see magnitude_highfive.cpp's
# own header comment for why), so this is a single srun step.
#
# Required env: BIN_DIR (hdf5_analysis_test/ directory, holding the built
#   magnitude_highfive binary), NUM_NODES, CLIENTS_PER_NODE,
#   CLIENT_TOTAL_TASKS, N_ELEM, RESULTS (final results_magnitude_highfive_<jobid>.csv
#   path -- see magnitude_highfive.sbatch), OUT_FILE (pre-striped path this
#   benchmark writes to)

set -xeu

pushd "$BIN_DIR"
srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_magnitude_highfive_${NUM_NODES}.log" \
  --error="client_magnitude_highfive_${NUM_NODES}.err" \
  ./magnitude_highfive "$N_ELEM" "$OUT_FILE"
popd

# magnitude_highfive prints one complete CSV line per timestep
# (N_TIMESTEPS=3), already including its own real close_s and
# step_total_s -- no write/analyze log-pairing or awk post-processing
# needed, unlike the plain-C posthoc pair's sbatch script.
lines=$(grep "^magnitude_highfive," "$BIN_DIR/client_magnitude_highfive_${NUM_NODES}.log" || true)
if [ -z "$lines" ]; then
  echo "magnitude_highfive,FAILED,${CLIENT_TOTAL_TASKS},${N_ELEM},FAILED" >> "$RESULTS"
else
  echo "$lines" >> "$RESULTS"
fi
