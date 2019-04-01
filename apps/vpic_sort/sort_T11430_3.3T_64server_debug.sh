#!/bin/bash -l

# #SBATCH -p regular
# #SBATCH -t 3:30:00
# #SBATCH --qos=premium
#SBATCH -p debug
#SBATCH -t 0:30:00
#SBATCH -N 64
#SBATCH --gres=craynetwork:2
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J sort_vpic3.3T_64server
#SBATCH -A m2621
#SBATCH -o o%j.sort_vpic_3.3T_import_64server_debug
#SBATCH -e o%j.sort_vpic_3.3T_import_64server_debug

# set -eu

ulimit -n 63536

###########       Programs Location      ############
FILEDIR=/global/cscratch1/sd/houhun/vpic_allOST_16MB/


###########       Start of runs         ############
echo "Start"
echo "====="
date

srun -N 64 -n 2048 ./vpic_sort $FILEDIR/eparticle_T11430_1.1_filter.h5p $FILEDIR/eparticle_T11430_1.1_filter_sort.h5

date
echo "The End"
echo "======="
