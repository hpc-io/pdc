#!/bin/bash -l

#REGSBATCH -q regular
#DBGSBATCH -q debug
#SBATCH -N NODENUM
#REGSBATCH -t 3:00:00
#DBGSBATCH -t 0:30:00
#SBATCH -C cpu
#SBATCH -J JOBNAME
#SBATCH -A PROJNAME
#SBATCH -o o%j.JOBNAME.out
#SBATCH -e o%j.JOBNAME.out

# export PDC_DEBUG=0

# This is a script for running PDC in shared mode on Perlmutter
# When running in Shared mode, the client processes and server processes are running on the same node.
# By alternating the number of server processes and the number client processes, you should be able to change the C/S ratio.
# You can simply set the number of server processes, and let the script to calculate the number of client processes.

# Per node configuration of your HPC system. 
MAX_PYSICAL_CORE=MPHYSICALCORE
MAX_HYPERTHREADING=MHYPERTHREADING

# Designated number of threads per process on each node 
# (this should be associated with -c option in srun)
NUM_THREAD_PER_SERVER_PROC=NTHREAD_PER_SPROC
NUM_THREAD_PER_CLIENT_PROC=NTHREAD_PER_CPROC


# Designated number of processes for server anc client on each node
# (this should be associated with -n option in srun)
NUM_SERVER_PROC_PER_NODE=N_SERVER_PROC
NUM_CLIENT_PROC_PER_NODE=N_CLIENT_PROC

# test if the number of threads is no larger than the total number of logical cores
TOTAL_NUM_PROC_PER_NODE=$((NUM_THREAD_PER_SERVER_PROC * NUM_SERVER_PROC_PER_NODE + NUM_THREAD_PER_CLIENT_PROC * NUM_CLIENT_PROC_PER_NODE))
TOTAL_NUM_LOGICAL_CORE_PER_NODE=$((MAX_PYSICAL_CORE * MAX_HYPERTHREADING))
if [[ "$TOTAL_NUM_PROC_PER_NODE" -gt "$TOTAL_NUM_LOGICAL_CORE_PER_NODE" ]]; then
    echo "Error: TOTAL_NUM_PROC_PER_NODE is larger than TOTAL_NUM_LOGICAL_CORE_PER_NODE"
    TOTAL_AVAILABLE_CORE=$((TOTAL_NUM_LOGICAL_CORE_PER_NODE - NUM_THREAD_PER_SERVER_PROC * NUM_SERVER_PROC_PER_NODE))
    NUM_CLIENT_PROC_PER_NODE=$(( TOTAL_AVAILABLE_CORE  / NUM_THREAD_PER_CLIENT_PROC))
    echo "fixing the number of client processes to $NUM_CLIENT_PROC_PER_NODE"
fi

# Set the number of times the test should be repeated.
REPEAT=1

# calculate the number of total processes for both server side and client side.
N_NODE=NODENUM
NCLIENT=$((NUM_CLIENT_PROC_PER_NODE * N_NODE))
NSERVER=$((NUM_SERVER_PROC_PER_NODE * N_NODE))

USE_DART=USING_DART
Q_TYPE=QUERY_TYPE
COM_TYPE=COMMUNICATION_TYPE

# clean up the PDC tmp directory
export PDC_TMPDIR=$SCRATCH/data/pdc/conf
export PDC_TMPDIR=${PDC_TMPDIR}/$N_NODE/$USE_DART/$Q_TYPE/$COM_TYPE
rm -rf $PDC_TMPDIR/*
mkdir -p $PDC_TMPDIR

EXECPATH=/global/cfs/cdirs/m2621/wzhang5/perlmutter/install/pdc/share/test/bin
TOOLPATH=/global/cfs/cdirs/m2621/wzhang5/perlmutter/install/pdc/share/test/bin
SERVER=$EXECPATH/pdc_server.exe
CLIENT=$TOOLPATH/kvtag_query_scale_col
CLOSE=$EXECPATH/close_server

chmod +x $EXECPATH/*
chmod +x $TOOLPATH/*

CSV_FILE=$SCRATCH/data/llsm/metadata/llsm_metadata.csv


date

# OpenMP settings: 
# set the OPENMP thread number to the smaller number between $NUM_THREAD_PER_SERVER_PROC and $NUM_THREAD_PER_CLIENT_PROC
export OMP_NUM_THREADS=$((NUM_THREAD_PER_SERVER_PROC < NUM_THREAD_PER_CLIENT_PROC ? NUM_THREAD_PER_SERVER_PROC : NUM_THREAD_PER_CLIENT_PROC))
export OMP_PLACES=threads
export OMP_PROC_BIND=close

echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"
echo "NSERVER=$NSERVER"
echo "NUM_THREAD_PER_SERVER_PROC=$NUM_THREAD_PER_SERVER_PROC"
echo "NCLIENT=$NCLIENT"
echo "NUM_THREAD_PER_CLIENT_PROC=$NUM_THREAD_PER_CLIENT_PROC"

export ATP_ENABLED=1

TOTAL_OBJ=1000000
ROUND=100
EXPAND_FACTOR=1000

echo ""
echo "============="
echo "$i Init server"
echo "============="
stdbuf -i0 -o0 -e0 srun -N $N_NODE -n $NSERVER -c $NUM_THREAD_PER_SERVER_PROC  --cpu_bind=cores $SERVER &
sleep 5


echo "============================================"
echo "KVTAGS with $N_NODE nodes"
echo "============================================"
stdbuf -i0 -o0 -e0 srun -N $N_NODE -n $NCLIENT -c $NUM_THREAD_PER_CLIENT_PROC --cpu_bind=cores $CLIENT $TOTAL_OBJ $ROUND $EXPAND_FACTOR $USE_DART $Q_TYPE $COM_TYPE $CSV_FILE

echo ""
echo "================="
echo "$i Closing server"
echo "================="
stdbuf -i0 -o0 -e0 srun -N $N_NODE -n $NSERVER -c 2 --mem=25600 --cpu_bind=cores $CLOSE


echo ""
echo "============="
echo "$i restart server"
echo "============="
stdbuf -i0 -o0 -e0 srun -N $N_NODE -n $NSERVER -c $NUM_THREAD_PER_SERVER_PROC  --cpu_bind=cores $SERVER restart &
sleep 5

date
