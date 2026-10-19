#!/bin/bash

# This script is useful for running vpicio within interactive jobs

set -xue

# Set environment variables
export NUM_NODES=$SLURM_JOB_NUM_NODES
export DATA_SERVERS_PER_NODE=1
export CLIENTS_PER_NODE=32
export SERVER_TOTAL_TASKS=$((NUM_NODES * DATA_SERVERS_PER_NODE))
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))
export BIN_DIR=/pscratch/sd/n/nlewi26/src/work_space/source/pdc/build/bin
export PDC_DATA_LOC=/pscratch/sd/n/nlewi26/src/work_space/install/pdc

# Print them
echo "NUM_NODES=$NUM_NODES"
echo "DATA_SERVERS_PER_NODE=$DATA_SERVERS_PER_NODE"
echo "CLIENTS_PER_NODE=$CLIENTS_PER_NODE"
echo "SERVER_TOTAL_TASKS=$SERVER_TOTAL_TASKS"
echo "CLIENT_TOTAL_TASKS=$CLIENT_TOTAL_TASKS"
echo "BIN_DIR=$BIN_DIR"
echo "TRANSFORM=$TRANSFORM"
echo "PDC_DATA_LOC=$PDC_DATA_LOC"

echo "running server script"
time ./srun_server.sh vpicio

echo "running client script"
time ./srun_client.sh vpicio

echo "closing server script"
time ./srun_close_server.sh vpicio

echo "logging data dir"
# Example output: filepath bytes stripecount stripesize ost_index
# ./.git/objects/34/272d525902ea41c81b5df0ca2542c0d4f34aa3 315 1 1048576 254
find $PDC_DATA_LOC/pdc_data -type f -exec sh -c '
  for f do
    s=$(stat -c %s "$f")
    si=$(lfs getstripe "$f" 2>/dev/null)
    c=$(echo "$si" | awk "/lmm_stripe_count/ {print \$2}")
    sz=$(echo "$si" | awk "/lmm_stripe_size/ {print \$2}")
    o=$(echo "$si" | awk "/^[[:space:]]*[0-9]+[[:space:]]/ {print \$1}" | paste -sd, -)
    echo -e "$f\t$s\t${c:-N/A}\t${sz:-N/A}\t${o:-N/A}"
  done
' sh {} +

echo "running server script"
time ./srun_server_restart.sh bdcats

echo "running client script"
time ./srun_client.sh bdcats

echo "closing server script"
time ./srun_close_server.sh bdcats

mkdir ./$TRANSFORM/$SLURM_JOB_ID
mv *.out /$TRANSFORM/$SLURM_JOB_ID
cp $BIN_DIR/*.out ./$TRANSFORM/$SLURM_JOB_ID
cp $BIN_DIR/*.err ./$TRANSFORM/$SLURM_JOB_ID