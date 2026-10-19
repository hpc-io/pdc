#!/bin/bash
# ADIOS2 ZFP-operator analog of Data Flyway's VPIC-IO + ZFP
# compression workload (pdc_helper_scripts/vpicio_scripts/,
# src/tests/misc/vpicio.c). Single node-count step, 32 client
# ranks/node, no server.
#
# Data volume per rank per step matches VPIC-IO's real configuration
# exactly, not just approximately: VPIC-IO writes NPARTICLES=8,388,608
# particles/rank (evaluation.tex's own "Particles/Rank: 8,388,608"),
# each particle 7 float fields + 1 int field = 32 bytes
# (src/tests/misc/vpicio.c: dx,dy,dz,ux,uy,uz,q as float + id as int),
# so 8,388,608 * 32 = 268,435,456 bytes/rank/step -- this workload's
# single flat float array carries the identical byte count via
# N_ELEM = 268,435,456 / 4 = 67,108,864 elements, so the compression
# operator sees the same amount of data per rank per step VPIC-IO's
# own ZFP transform does, even though the array shape (one flat float
# buffer vs. 8 separate particle-attribute arrays) differs -- ADIOS2
# Operators attach per-Variable, not per-write-call, so this doesn't
# change what's being measured (compression throughput on N bytes),
# only how those bytes are laid out client-side.
#
# Submit one of these per node count via compression_analysis_run.sh,
# or directly, e.g.: sbatch --nodes=4 compression_analysis.sbatch
# Override N_ELEM at submit time, e.g.:
#   N_ELEM=33554432 sbatch --nodes=4 compression_analysis.sbatch

#SBATCH --job-name=adios2-compression
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=32
#SBATCH --cpus-per-task=4
#SBATCH --account=m2621
#SBATCH --time=00:30:00
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --output=%x_%j.log

set -xeu
cd "$SLURM_SUBMIT_DIR"

export NUM_NODES=$SLURM_JOB_NUM_NODES
export CLIENTS_PER_NODE=32
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

export BIN_DIR=${BIN_DIR:-$PWD}
export OUT_DIR=${OUT_DIR:-$SCRATCH/adios2_analysis_bench}
mkdir -p "$OUT_DIR"

export N_ELEM=${N_ELEM:-67108864}   # 256 MiB/rank/step, matching VPIC-IO's
                                     # real 8,388,608 particles/rank * 32
                                     # bytes/particle -- see comment above.
export OUT_FILE=${OUT_DIR}/adios2_bench_compression_${NUM_NODES}.bp
export RESULTS=$PWD/results_adios2_compression_${SLURM_JOB_ID}.csv

STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((N_ELEM * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
rm -rf "$OUT_FILE"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_DIR" \
  || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,n_elem,setup_s,write_s,confirm_read_s,step_total_s,bad" > "$RESULTS"

srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_compression_${NUM_NODES}.log" \
  --error="client_compression_${NUM_NODES}.err" \
  "$BIN_DIR/adios2_bench_compression" "$N_ELEM" "$OUT_FILE"

grep "^adios2_zfp," "client_compression_${NUM_NODES}.log" >> "$RESULTS" || true


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
