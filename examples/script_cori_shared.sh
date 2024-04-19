#!/bin/bash -l
#SBATCH -p regular
#SBATCH -N 4
#SBATCH -t 0:15:00
#SBATCH --gres=craynetwork:2
#SBATCH -L SCRATCH
#SBATCH -C knl,quad,cache
#SBATCH -J qout
#SBATCH -A mxxx
#SBATCH -o qout.%j
#SBATCH -e qout.%j

ulimit -n 63536
COMMON_CMD="--mem=51200 --overlap"
SERVER_COMMON_CMD="--mem=21600 --overlap"
###########       Programs Location      ############
OUTDIR=$SCRATCH/VPIC
#-N $SLURM_JOB_NUM_NODES
SERVERS=4
CLIENT_NODES=4
CLIENTS_PER_NODE=32
CLIENTS=$(($CLIENT_NODES*$CLIENTS_PER_NODE))
export PDC_DIR="$HOME/pdc_develop/pdc/install"
export MERCURY_DIR="$HOME/pdc_develop/mercury/install"
export LIBFABRIC_DIR="$HOME/pdc_develop/libfabric-1.11.2/install"
export LD_LIBRARY_PATH="$LIBFABRIC/lib:$MERCURY_DIR/lib:$PDC_DIR/lib:$LD_LIBRARY_PATH"
rm -rf $OUTDIR/*

PARTITION_METHOD=1

cp $PDC_DIR/bin/pdc_server.exe $OUTDIR
cp $PDC_DIR/bin/close_server $OUTDIR
cp vpicio_batch $OUTDIR
export SUBMIT_DIR=$(pwd)
cd $OUTDIR
echo $(pwd)
rm -rf $OUTDIR/*.csv $SUBMIT_DIR/vpicio_batch_no_delay/* $SUBMIT_DIR/vpicio_batch_delay/*
export cmd="srun -N $SERVERS -n $SERVERS -c 64 --cpu_bind=cores $SERVER_COMMON_CMD ./pdc_server.exe"
echo $cmd
$cmd &
sleep 3
export cmd="srun -N $CLIENT_NODES -n $CLIENTS -c 2 --cpu_bind=cores $COMMON_CMD ./vpicio_batch 0 0 524288 2 $PARTITION_METHOD 0"
echo $cmd
$cmd
sleep 2
export cmd="srun -N 1 -n 1 $COMMON_CMD -c 2 --cpu_bind=cores ./close_server"
echo $cmd
$cmd
cp $OUTDIR/*.csv $SUBMIT_DIR/vpicio_batch_no_delay
rm -rf $OUTDIR/pdc_data
rm -rf $OUTDIR/pdc_tmp
rm -rf $OUTDIR/*.csv

export cmd="srun -N $SERVERS -n $SERVERS -c 64 --cpu_bind=cores $SERVER_COMMON_CMD ./pdc_server.exe"
echo $cmd
$cmd &
sleep 3
export cmd="srun -N $CLIENT_NODES -n $CLIENTS -c 2 --cpu_bind=cores $COMMON_CMD ./vpicio_batch 10 0 524288 2 $PARTITION_METHOD 0"
echo $cmd
$cmd
sleep 2
export cmd="srun -N 1 -n 1 $COMMON_CMD -c 2 --cpu_bind=cores ./close_server"
echo $cmd
$cmd
sleep 2
cp $OUTDIR/*.csv $SUBMIT_DIR/vpicio_batch_delay

rm -rf $OUTDIR/pdc_data
rm -rf $OUTDIR/pdc_tmp
rm -rf $OUTDIR/*.csv
