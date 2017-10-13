#!/bin/bash -l

#SBATCH -p debug
#SBATCH -N 8
#SBATCH -t 0:30:00
#SBATCH --gres=craynetwork:2
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH -J h5boss_query
#SBATCH -A m2621
#SBATCH -o o%j.h5boss_query
#SBATCH -e o%j.h5boss_query
# #DW jobdw capacity=2000GB access_mode=striped type=scratch pool=sm_pool

# Using burst buffer for checkpoint
# mkdir $DW_JOB_STRIPED/pdc_tmp
# export PDC_TMPDIR=$DW_JOB_STRIPED/pdc_tmp


REPEAT=1

N_NODE=2
#NCLIENT=32
NCLIENT=1

let NSERVER=$N_NODE*2
let TOTALPROC=$NCLIENT*$N_NODE

SERVER=/global/homes/w/wzhang5/software/newSoMeta/src/build/bin/pdc_server.exe
CLOSE= /global/homes/w/wzhang5/software/newSoMeta/src/build/bin/close_server
IMPORT=/global/homes/w/wzhang5/software/newSoMeta/apps/h5boss/build/h5boss_import.exe
SEARCH=/global/homes/w/wzhang5/software/newSoMeta/apps/h5boss/build/h5boss_query_random.exe

PMLIST=/global/homes/w/wzhang5/software/SoMeta/apps/h5boss/pm_list_2000.txt

COMMON_CMD="--cpu_bind=cores -c 2 --mem=10240 --gres=craynetwork:1"

QUERY=( 20 50 100 120 150 200 )
#QUERY=( 50000 100000 150000 200000 250000 300000 350000 400000 )
#QUERY=( 26000 52000 104000 208000 416000 832000 )

date
# echo ""
# echo "=============="
# echo "Restart server"
# echo "=============="
# srun -N $N_NODE -n $NSERVER $COMMON_CMD $SERVER restart  &
# sleep 15


for (( i = 0; i < $REPEAT ; i++ )); do

    echo ""
    echo "==========="
    echo "Init server"
    echo "==========="
    srun -N $N_NODE -n $NSERVER $COMMON_CMD $SERVER  &
    sleep 15

    echo "============================"
    echo "$i Importing H5BOSS Metadata"
    echo "============================"
    srun -N $N_NODE -n $TOTALPROC $COMMON_CMD $IMPORT $PMLIST
    sleep 5

    for nquery in "${QUERY[@]}"; do

        echo ""
        echo "==================="
        echo "$i Querying $nquery"
        echo "==================="
        srun -N $N_NODE -n $TOTALPROC $COMMON_CMD $SEARCH $PMLIST $nquery
        sleep 5

    done

    echo "===================="
    echo "Close and Checkpoint"
    echo "===================="
    srun -N 1 -n 1 -c 2           $COMMON_CMD $CLOSE
    sleep 5

done

# rm -rf ./pdc_tmp

date
