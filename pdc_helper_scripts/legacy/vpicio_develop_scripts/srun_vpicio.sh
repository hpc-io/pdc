#!/bin/bash

set -xue

export NUM_NODES=2
export DATA_SERVERS_PER_NODE=2
export CLIENTS_PER_NODE=126
export PDC_DATA_LOC=/pscratch/sd/n/nlewi26/src/work_space/install/
export PDC_TMPDIR=/pscratch/sd/n/nlewi26/src/work_space/install/

echo "clearing data and tmp dir"
rm -rf $PDC_DATA_LOC/pdc_data $PDC_TMPDIR/pdc_tmp

echo "removing previous logs"
rm -rf *.err *.log

echo "NUM_NODES=$NUM_NODES"
echo "DATA_SERVERS_PER_NODE=$DATA_SERVERS_PER_NODE"
echo "CLIENTS_PER_NODE=$CLIENTS_PER_NODE"

export SERVER_TOTAL_TASKS=$((NUM_NODES * DATA_SERVERS_PER_NODE))
export CLIENT_TOTAL_TASKS=$((NUM_NODES * CLIENTS_PER_NODE))

echo "SERVER_TOTAL_TASKS=$SERVER_TOTAL_TASKS"
echo "CLIENT_TOTAL_TASKS=$CLIENT_TOTAL_TASKS"

echo "running server script"
./srun_server.sh

echo "running client script"
./srun_client.sh

echo "running server close script"
./srun_close_server.sh
