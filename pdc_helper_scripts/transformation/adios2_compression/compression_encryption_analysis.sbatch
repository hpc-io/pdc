#!/bin/bash
# ADIOS2 ZFP+EncryptionOperator analog of Data Flyway's VPIC-IO +
# ZFP + libsodium-encryption chained transform (zfp_libsod.json). Same
# per-rank data volume derivation as compression_analysis.sbatch (see
# that file's comment) -- N_ELEM defaults identically.
#
# Submit one of these per node count via
# compression_encryption_analysis_run.sh, or directly, e.g.:
#   sbatch --nodes=4 compression_encryption_analysis.sbatch

#SBATCH --job-name=adios2-compression-encryption
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

export N_ELEM=${N_ELEM:-67108864}   # same derivation as compression_analysis.sbatch
export OUT_FILE=${OUT_DIR}/adios2_bench_compression_encryption_${NUM_NODES}.bp
export RESULTS=$PWD/results_adios2_compression_encryption_${SLURM_JOB_ID}.csv

STRIPE_COUNT=128
STRIPE_SIZE_BYTES=$((N_ELEM * 4))
STRIPE_SIZE_BYTES=$(( ((STRIPE_SIZE_BYTES + 65535) / 65536) * 65536 ))
rm -rf "$OUT_FILE" "${OUT_FILE}.key"
lfs setstripe -c "$STRIPE_COUNT" -S "$STRIPE_SIZE_BYTES" "$OUT_DIR" \
  || echo "WARNING: lfs setstripe failed, falling back to filesystem default layout"

echo "mode,step,n_ranks,n_elem,setup_s,write_s,confirm_read_s,step_total_s,bad" > "$RESULTS"

srun \
  -N "$NUM_NODES" \
  -n "$CLIENT_TOTAL_TASKS" \
  --ntasks-per-node="$CLIENTS_PER_NODE" \
  --output="client_compression_encryption_${NUM_NODES}.log" \
  --error="client_compression_encryption_${NUM_NODES}.err" \
  "$BIN_DIR/adios2_bench_compression_encryption" "$N_ELEM" "$OUT_FILE"

grep "^adios2_zfp_encrypt," "client_compression_encryption_${NUM_NODES}.log" >> "$RESULTS" || true


# Prepend servers_per_node as the first column (0 -- no PDC server involved).
sed -i "1s/^/servers_per_node,/; 2,\$s/^/0,/" "$RESULTS"

echo "Done. Results in $RESULTS"
