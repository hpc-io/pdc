#!/bin/bash

MIN_PROC=16
MAX_PROC=128

PROG_BASENAME=kvqry

curdir=$(pwd)

first_submit=1

i_type=$1
q_type=$2
c_type=$3

for (( i = $MIN_PROC; i <= $MAX_PROC; i*=2 )); do
    
    cd $curdir/$i

    JOBNAME=${PROG_BASENAME}_${i}_${i_type}_${q_type}_${c_type}
    TARGET=$JOBNAME.sbatch

    njob=`squeue -u $USER | grep ${PROG_BASENAME} | wc -l`
    echo $njob
    while [ $njob -ge 4 ]
    do
        sleeptime=$[ ( $RANDOM % 1000 )  ]
        sleep $sleeptime
        njob=`squeue -u $USER | grep ${PROG_BASENAME} | wc -l`
        echo $njob
    done

    if [[ $first_submit == 1 ]]; then
        # Submit first job w/o dependency
        echo "Submitting $TARGET"
        job=`sbatch $TARGET`
        first_submit=0
    else
        echo "Submitting $TARGET after ${job: -8}"
        job=`sbatch -d afterany:${job: -8} $TARGET`
    fi

    sleeptime=$[ ( $RANDOM % 5 )  ]
    sleep $sleeptime
    
done
